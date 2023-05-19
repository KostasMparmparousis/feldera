use super::KafkaLogLevel;
use crate::{OutputEndpoint, OutputTransport};
use anyhow::{Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use log::debug;
use rdkafka::{
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext,
};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, collections::BTreeMap, env, time::Duration};
use utoipa::{
    openapi::{
        schema::{KnownFormat, Schema},
        ObjectBuilder, RefOr, SchemaFormat, SchemaType,
    },
    ToSchema,
};

const OUTPUT_POLLING_INTERVAL: Duration = Duration::from_millis(100);

/// [`OutputTransport`] implementation that writes data to a Kafka topic.
///
/// This output transport is only available if the crate is configured with
/// `with-kafka` feature.
///
/// The output transport factory gives this transport the name `kafka`.
pub struct KafkaOutputTransport;

impl OutputTransport for KafkaOutputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("kafka")
    }

    /// Creates a new [`OutputEndpoint`] fpor writing to a Kafka topic,
    /// interpreting `config` as a [`KafkaOutputConfig`].
    ///
    /// See [`OutputTransport::new_endpoint()`] for more information.
    fn new_endpoint(
        &self,
        _name: &str,
        config: &YamlValue,
        async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<Box<dyn OutputEndpoint>> {
        let config = KafkaOutputConfig::deserialize(config)?;
        let ep = KafkaOutputEndpoint::new(config, async_error_callback)?;

        Ok(Box::new(ep))
    }
}

const fn default_max_inflight_messages() -> u32 {
    1000
}

/// Configuration for writing data to a Kafka topic with [`OutputTransport`].
#[derive(Deserialize, Debug)]
pub struct KafkaOutputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka producer.
    #[serde(flatten)]
    pub kafka_options: BTreeMap<String, String>,

    /// Topic to write to.
    pub topic: String,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    pub log_level: Option<KafkaLogLevel>,

    /// Maximum number of unacknowledged messages buffered by the Kafka
    /// producer.
    ///
    /// Kafka producer buffers outgoing messages until it receives an
    /// acknowledgement from the broker.  This configuration parameter
    /// bounds the number of unacknowledged messages.  When the number of
    /// unacknowledged messages reaches this limit, sending of a new message
    /// blocks until additional acknowledgements arrive from the broker.
    ///
    /// Defaults to 1000.
    #[serde(default = "default_max_inflight_messages")]
    pub max_inflight_messages: u32,
}

impl KafkaOutputConfig {
    /// Set `option` to `val`, if missing.
    fn set_option_if_missing(&mut self, option: &str, val: &str) {
        self.kafka_options
            .entry(option.to_string())
            .or_insert_with(|| val.to_string());
    }

    /// Validate configuration, set default option values required by this
    /// adapter.
    fn validate(&mut self) -> AnyResult<()> {
        self.set_option_if_missing(
            "bootstrap.servers",
            &env::var("REDPANDA_BROKERS").unwrap_or_else(|_| "localhost".to_string()),
        );
        Ok(())
    }
}

// The auto-derived implementation gets confused by the flattened
// `kafka_options` field.
impl<'s> ToSchema<'s> for KafkaOutputConfig {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "KafkaOutputConfig",
            ObjectBuilder::new()
                .property(
                    "topic",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                )
                .required("topic")
                .property(
                    "log_level",
                    KafkaLogLevel::schema().1
                )
                .property(
                    "max_inflight_messages",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Integer)
                        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
                        .description(Some(r#"Maximum number of unacknowledged messages buffered by the Kafka producer.

Kafka producer buffers outgoing messages until it receives an
acknowledgement from the broker.  This configuration parameter
bounds the number of unacknowledged messages.  When the number of
unacknowledged messages reaches this limit, sending of a new message
blocks until additional acknowledgements arrive from the broker.

Defaults to 1000."#)),
                )
                .additional_properties(Some(
                        ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                        .description(Some(r#"Options passed directly to `rdkafka`.

See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
used to configure the Kafka producer."#))))
                .into(),
        )
    }
}
/// Producer context object used to handle async delivery notifications from
/// Kafka.
struct KafkaOutputContext {
    /// Used to unpark the endpoint thread waiting for the number of in-flight
    /// messages to drop below `max_inflight_messages`.
    unparker: Unparker,

    /// Callback to notify the controller about delivery failure.
    async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
}

impl KafkaOutputContext {
    fn new(
        unparker: Unparker,
        async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> Self {
        Self {
            unparker,
            async_error_callback,
        }
    }
}

impl ClientContext for KafkaOutputContext {}

impl ProducerContext for KafkaOutputContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        if let Err((error, _message)) = delivery_result {
            (self.async_error_callback)(false, AnyError::new(error.clone()));
        }

        // There is no harm in unparking the endpoint thread unconditionally,
        // regardless of whether it's actually parked or not.
        self.unparker.unpark();
    }
}

struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    topic: String,
    max_inflight_messages: u32,
    parker: Parker,
}

impl KafkaOutputEndpoint {
    fn new(
        mut config: KafkaOutputConfig,
        async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<Self> {
        // Create Kafka producer configuration.
        config.validate()?;
        debug!("Starting Kafka output endpoint: {config:?}");

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, value);
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(RDKafkaLogLevel::from(log_level));
        }

        let parker = Parker::new();

        // Context object to intercept message delivery events.
        let context = KafkaOutputContext::new(parker.unparker().clone(), async_error_callback);

        // Create Kafka producer.
        let kafka_producer = ThreadedProducer::from_config_and_context(&client_config, context)?;

        Ok(Self {
            kafka_producer,
            topic: config.topic,
            max_inflight_messages: config.max_inflight_messages,
            parker,
        })
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        // Wait for the number of unacknowledged messages to drop
        // below `max_inflight_messages`.
        while self.kafka_producer.in_flight_count() as i64 > self.max_inflight_messages as i64 {
            // FIXME: It appears that the delivery callback can be invoked before the
            // in-flight counter is decremented, in which case we may never get
            // unparked and may need to poll the in-flight counter.  This
            // shouldn't cause performance issues in practice, but
            // it would still be nice to have a more reliable way to wake up the endpoint
            // thread _after_ the in-flight counter has been decremented.
            self.parker.park_timeout(OUTPUT_POLLING_INTERVAL);
        }

        let record = <BaseRecord<(), [u8], ()>>::to(&self.topic).payload(buffer);
        self.kafka_producer
            .send(record)
            .map_err(|(err, _record)| err)?;
        Ok(())
    }
}
