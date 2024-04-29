/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ResourceConfig } from './ResourceConfig'
/**
 * Global pipeline configuration settings. This is the publicly
 * exposed type for users to configure pipelines.
 */
export type RuntimeConfig = {
  /**
   * Enable CPU profiler.
   */
  cpu_profiler?: boolean
  /**
   * Maximal delay in microseconds to wait for `min_batch_size_records` to
   * get buffered by the controller, defaults to 0.
   */
  max_buffering_delay_usecs?: number
  /**
   * Minimal input batch size.
   *
   * The controller delays pushing input records to the circuit until at
   * least `min_batch_size_records` records have been received (total
   * across all endpoints) or `max_buffering_delay_usecs` microseconds
   * have passed since at least one input records has been buffered.
   * Defaults to 0.
   */
  min_batch_size_records?: number
  resources?: ResourceConfig
  /**
   * Should persistent storage be enabled for this pipeline?
   *
   * - If `true`, the pipeline state is stored in the specified location,
   * is persisted across restarts, and can be checkpointed and recovered.
   *
   * - If `false`, the pipeline's state is kept in in-memory data-structures.
   * This is useful if the pipeline is ephemeral and does not need to be recovered
   * after a restart. The pipeline will most likely run faster since it does not
   * need to read from, or write to disk
   */
  storage?: boolean
  /**
   * Number of DBSP worker threads.
   */
  workers?: number
}
