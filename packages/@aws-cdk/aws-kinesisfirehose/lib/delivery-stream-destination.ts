import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';

/**
 * A delivery stream destination configuration
 */
export interface DeliveryStreamDestinationConfig {
  /**
   * Schema-less properties that will be injected directly into `CfnDeliveryStream`.
   */
  readonly properties: object;
}

/**
 * Options when binding a destination to a delivery stream
 */
export interface DeliveryStreamDestinationBindOptions {
  /**
   * The IAM role associated with the delivery stream
   */
  readonly role: iam.IRole;
  /**
   * The S3 bucket where Kinesis Data Firehose backs up data going to the destination.
   */
  readonly bucket: s3.IBucket;
}

/**
 * A Kinesis Firehose Delivery Stream destination
 */
export interface IDeliveryStreamDestination {
  /**
   * Binds this destination to the Kinesis Firehose delivery stream
   */
  bind(options: DeliveryStreamDestinationBindOptions): DeliveryStreamDestinationConfig;
}