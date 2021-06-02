import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import { IResource, RemovalPolicy, Resource } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { IDeliveryStreamDestination } from './delivery-stream-destination';
import { CfnDeliveryStream } from './kinesisfirehose.generated';

export interface IDeliveryStream extends IResource {
  /**
   * The ARN of the delivery stream.
   *
   * @attribute
   */
   readonly streamArn: string;

   /**
    * The name of the delivery stream
    *
    * @attribute
    */
   readonly streamName: string;

   readonly role: iam.Role;

   readonly bucket: s3.Bucket;

   addElasticSearchDestination(configuration: CfnDeliveryStream.ElasticsearchDestinationConfigurationProperty): void;
}

export interface DeliveryStreamProps {

  readonly deliveryStreamType?: string;

  readonly destination: IDeliveryStreamDestination;

  readonly role?: iam.Role;

  readonly bucket?: s3.Bucket;
}

export class DeliveryStream extends Resource implements IDeliveryStream {

  public readonly streamArn: string;
  public readonly streamName: string;
  public readonly role: iam.Role;
  public readonly bucket: s3.Bucket;

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

    this.deliveryStream = new CfnDeliveryStream(this, 'Resource', {
      deliveryStreamType: props.deliveryStreamType ?? 'DirectPut',
    });
    this.deliveryStream.node.addDependency(this.role);

    props.destination.bind(this);

    this.streamArn = this.getResourceArnAttribute(this.deliveryStream.attrArn, {
      service: 'kinesis',
      resource: 'deliverystream',
      resourceName: this.physicalName,
    });
    this.streamName = this.getResourceNameAttribute(this.deliveryStream.ref);
  }

  public addElasticSearchDestination(configuration: CfnDeliveryStream.ElasticsearchDestinationConfigurationProperty) {
    this.deliveryStream.elasticsearchDestinationConfiguration = configuration;
  }
}