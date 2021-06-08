import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import { IResource, RemovalPolicy, Resource } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { IDeliveryStreamDestination } from './delivery-stream-destination';
import { CfnDeliveryStream } from './kinesisfirehose.generated';

/**
 * Represents a Kinesis Data Firehose delivery stream.
 */
export interface IDeliveryStream extends IResource {
  /**
   * The ARN of the delivery stream.
   *
   * @attribute
   */
  readonly deliveryStreamArn: string;

  /**
    * The name of the delivery stream
    *
    * @attribute
    */
  readonly deliveryStreamName: string;

  /**
   * TODO Add doc
   */
  readonly role: iam.IRole;

  /**
   * TODO Add doc
   */
  readonly bucket: s3.IBucket;

  /**
   * TODO Add doc
   */
  addElasticSearchDestination(configuration: CfnDeliveryStream.ElasticsearchDestinationConfigurationProperty): void;

  /**
   * TODO Add doc
   */
  addS3Destination(configuration: CfnDeliveryStream.S3DestinationConfigurationProperty): void;
}

/**
 * Properties for a new delivery stream
 */
export interface DeliveryStreamProps {

  /**
   * The name of the delivery stream
   *
   * @attribute
   */
  readonly deliveryStreamName: string;

  /**
   * The type of the delivery stream
   *
   * @attribute
   */
  readonly deliveryStreamType?: string;

  /**
   * TODO Add doc
   */
  readonly destination: IDeliveryStreamDestination;

  /**
   * TODO Add doc
   *
   * @default - TODO
   */
  readonly role?: iam.IRole;

  /**
   * TODO Add doc
   *
   * @default - TODO
   */
  readonly bucket?: s3.IBucket;
}

/**
 * TODO Add doc
 */
export class DeliveryStream extends Resource implements IDeliveryStream {

  /**
   * TODO Implement a fromXxx method
   */
  public static fromDeliveryStreamName(scope: Construct, id: string, deliveryStreamName: string) {
    return { scope, id, deliveryStreamName } as unknown as IDeliveryStream;
  }

  /**
   * TODO Add doc
   */
  public readonly deliveryStreamArn: string;

  /**
   * TODO Add doc
   */
  public readonly deliveryStreamName: string;

  /**
   * TODO Add doc
   */
  public readonly role: iam.IRole;

  /**
   * TODO Add doc
   */
  public readonly bucket: s3.IBucket;

  private readonly deliveryStream: CfnDeliveryStream;

  constructor(scope: Construct, id: string, props: DeliveryStreamProps) {
    super(scope, id);

    this.role = props.role || new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    this.bucket = props.bucket || new s3.Bucket(this, 'Bucket', {
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this.bucket.grantReadWrite(this.role);

    const destinationConfig = props.destination.bind(this);

    this.deliveryStream = new CfnDeliveryStream(this, 'Resource', {
      deliveryStreamType: props.deliveryStreamType ?? 'DirectPut',
      ...destinationConfig.properties,
    });
    this.deliveryStream.node.addDependency(this.role);

    this.deliveryStreamArn = this.getResourceArnAttribute(this.deliveryStream.attrArn, {
      service: 'kinesis',
      resource: 'deliverystream',
      resourceName: this.physicalName,
    });
    this.deliveryStreamName = this.getResourceNameAttribute(this.deliveryStream.ref);
  }

  public addElasticSearchDestination(configuration: CfnDeliveryStream.ElasticsearchDestinationConfigurationProperty) {
    this.deliveryStream.elasticsearchDestinationConfiguration = configuration;
  }

  public addS3Destination(configuration: CfnDeliveryStream.S3DestinationConfigurationProperty): void {
    this.deliveryStream.s3DestinationConfiguration = configuration;
  }
}