

use utoipa::ToSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, ToSchema)]
pub struct UdfRequest {
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UdfResponse {
    pub message: String,
}

use actix_web::{web, Responder};

pub async fn create_udf(udf: web::Json<UdfRequest>) -> impl Responder {
    let udf_filename = format!("{}.rs", udf.name);
    std::fs::write(&udf_filename, &udf.definition).expect("Unable to write UDF file");

    let output = std::process::Command::new("./sql-to-dbsp")
        .arg("test.sql")
        .arg("--udf")
        .arg(&udf_filename)
        .arg("--handles")
        .arg("-o")
        .arg("output.rs")
        .output()
        .expect("Failed to execute process");

    if output.status.success() {
        web::Json(UdfResponse { message: format!("UDF {} created", udf.name) })
    } else {
        let error_message = String::from_utf8_lossy(&output.stderr);
        web::Json(UdfResponse { message: format!("Failed to create UDF: {}", error_message) })
    }
}
