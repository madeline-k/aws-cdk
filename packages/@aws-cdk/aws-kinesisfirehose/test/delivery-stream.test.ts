import '@aws-cdk/assert-internal/jest';
import { ABSENT, arrayWith } from '@aws-cdk/assert-internal';
import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as iam from '@aws-cdk/aws-iam';
import * as kinesis from '@aws-cdk/aws-kinesis';
import * as kms from '@aws-cdk/aws-kms';
import * as cdk from '@aws-cdk/core';
import { Construct } from 'constructs';
import * as firehose from '../lib';

describe('delivery stream', () => {
  let stack: cdk.Stack;

  const bucketArn = 'arn:aws:s3:::my-bucket';
  const roleArn = 'arn:aws:iam::111122223333:role/my-role';
  const mockS3Destination: firehose.IDestination = {
    bind(_scope: Construct, _options: firehose.DestinationBindOptions): firehose.DestinationConfig {
      return {
        properties: {
          s3DestinationConfiguration: {
            bucketArn: bucketArn,
            roleArn: roleArn,
          },
        },
      };
    },
  };

  beforeEach(() => {
    stack = new cdk.Stack();
  });

  test('creates stream with default values', () => {
    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    expect(stack).toHaveResource('AWS::KinesisFirehose::DeliveryStream', {
      DeliveryStreamEncryptionConfigurationInput: ABSENT,
      DeliveryStreamName: ABSENT,
      DeliveryStreamType: 'DirectPut',
      KinesisStreamSourceConfiguration: ABSENT,
      S3DestinationConfiguration: {
        BucketARN: bucketArn,
        RoleARN: roleArn,
      },
    });
  });

  test('provided role is set as grant principal', () => {
    const role = new iam.Role(stack, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      role: role,
    });

    expect(deliveryStream.grantPrincipal).toBe(role);
  });

  test('not providing role creates one', () => {
    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    expect(stack).toHaveResourceLike('AWS::IAM::Role', {
      AssumeRolePolicyDocument: {
        Statement: [
          {
            Principal: {
              Service: 'firehose.amazonaws.com',
            },
          },
        ],
      },
    });
  });

  test('providing source stream creates configuration and grants permission', () => {
    const sourceStream = new kinesis.Stream(stack, 'Source Stream');
    const role = new iam.Role(stack, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      sourceStream: sourceStream,
      role: role,
    });

    expect(stack).toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: arrayWith(
              'kinesis:DescribeStream',
              'kinesis:GetRecords',
              'kinesis:GetShardIterator',
              'kinesis:ListShards',
            ),
            Resource: stack.resolve(sourceStream.streamArn),
          },
        ],
      },
      Roles: [stack.resolve(role.roleName)],
    });
    expect(stack).toHaveResource('AWS::KinesisFirehose::DeliveryStream', {
      DeliveryStreamType: 'KinesisStreamAsSource',
      KinesisStreamSourceConfiguration: {
        KinesisStreamARN: stack.resolve(sourceStream.streamArn),
        RoleARN: stack.resolve(role.roleArn),
      },
    });
  });

  test('requesting customer-owned encryption creates key and configuration', () => {
    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.CUSTOMER_MANAGED,
    });

    expect(stack).toHaveResource('AWS::KMS::Key');
    expect(stack).toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: arrayWith(
              'kms:Encrypt',
              'kms:Decrypt',
            ),
            Resource: {
              'Fn::GetAtt': [
                'DeliveryStreamKey56A6407F',
                'Arn',
              ],
            },
          },
        ],
      },
      Roles: [{ Ref: 'DeliveryStreamServiceRole964EEBCC' }],
    });
    expect(stack).toHaveResource('AWS::KinesisFirehose::DeliveryStream', {
      DeliveryStreamType: 'DirectPut',
      DeliveryStreamEncryptionConfigurationInput: {
        KeyARN: {
          'Fn::GetAtt': [
            'DeliveryStreamKey56A6407F',
            'Arn',
          ],
        },
        KeyType: 'CUSTOMER_MANAGED_CMK',
      },
    });
  });

  test('providing encryption key creates configuration', () => {
    const key = new kms.Key(stack, 'Key');

    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      encryptionKey: key,
    });

    expect(stack).toHaveResource('AWS::KMS::Key');
    expect(stack).toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: arrayWith(
              'kms:Encrypt',
              'kms:Decrypt',
            ),
            Resource: {
              'Fn::GetAtt': [
                'Key961B73FD',
                'Arn',
              ],
            },
          },
        ],
      },
      Roles: [{ Ref: 'DeliveryStreamServiceRole964EEBCC' }],
    });
    expect(stack).toHaveResource('AWS::KinesisFirehose::DeliveryStream', {
      DeliveryStreamType: 'DirectPut',
      DeliveryStreamEncryptionConfigurationInput: {
        KeyARN: stack.resolve(key.keyArn),
        KeyType: 'CUSTOMER_MANAGED_CMK',
      },
    });
  });

  test('requesting AWS-owned key does not create key and creates configuration', () => {
    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.AWS_OWNED,
    });

    expect(stack).not.toHaveResource('AWS::KMS::Key');
    expect(stack).not.toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: arrayWith(
              'kms:Encrypt',
              'kms:Decrypt',
            ),
          },
        ],
      },
      Roles: [{ Ref: 'DeliveryStreamServiceRole964EEBCC' }],
    });
    expect(stack).toHaveResourceLike('AWS::KinesisFirehose::DeliveryStream', {
      DeliveryStreamType: 'DirectPut',
      DeliveryStreamEncryptionConfigurationInput: {
        KeyARN: ABSENT,
        KeyType: 'AWS_OWNED_CMK',
      },
    });
  });

  test('requesting no encryption creates no configuration', () => {
    new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.UNENCRYPTED,
    });

    expect(stack).not.toHaveResource('AWS::KMS::Key');
    expect(stack).not.toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: arrayWith(
              'kms:Encrypt',
              'kms:Decrypt',
            ),
          },
        ],
      },
      Roles: [{ Ref: 'DeliveryStreamServiceRole964EEBCC' }],
    });
    expect(stack).toHaveResourceLike('AWS::KinesisFirehose::DeliveryStream', {
      DeliveryStreamType: 'DirectPut',
      DeliveryStreamEncryptionConfigurationInput: ABSENT,
    });
  });

  test('requesting AWS-owned key and providing a key throws an error', () => {
    const key = new kms.Key(stack, 'Key');

    expect(() => new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.AWS_OWNED,
      encryptionKey: key,
    })).toThrowError('Specified stream encryption as AWS_OWNED but provided a customer-managed key');
  });

  test('requesting no encryption and providing a key throws an error', () => {
    const key = new kms.Key(stack, 'Key');

    expect(() => new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.UNENCRYPTED,
      encryptionKey: key,
    })).toThrowError('Specified stream encryption as UNENCRYPTED but provided a customer-managed key');
  });

  test('requesting encryption or providing a key when source is a stream throws an error', () => {
    const sourceStream = new kinesis.Stream(stack, 'Source Stream');

    expect(() => new firehose.DeliveryStream(stack, 'Delivery Stream 1', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.AWS_OWNED,
      sourceStream,
    })).toThrowError('Requested server-side encryption but delivery stream source is a Kinesis Data Stream. Specify server-side encryption on the Data Stream instead.');
    expect(() => new firehose.DeliveryStream(stack, 'Delivery Stream 2', {
      destinations: [mockS3Destination],
      encryption: firehose.StreamEncryption.CUSTOMER_MANAGED,
      sourceStream,
    })).toThrowError('Requested server-side encryption but delivery stream source is a Kinesis Data Stream. Specify server-side encryption on the Data Stream instead.');
    expect(() => new firehose.DeliveryStream(stack, 'Delivery Stream 3', {
      destinations: [mockS3Destination],
      encryptionKey: new kms.Key(stack, 'Key'),
      sourceStream,
    })).toThrowError('Requested server-side encryption but delivery stream source is a Kinesis Data Stream. Specify server-side encryption on the Data Stream instead.');
  });

  test('grant provides access to stream', () => {
    const role = new iam.Role(stack, 'Role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    deliveryStream.grant(role, 'firehose:PutRecord');

    expect(stack).toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: 'firehose:PutRecord',
            Resource: stack.resolve(deliveryStream.deliveryStreamArn),
          },
        ],
      },
      Roles: [stack.resolve(role.roleName)],
    });
  });

  test('grantWrite provides PutRecord* access to stream', () => {
    const role = new iam.Role(stack, 'Role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    deliveryStream.grantWrite(role);

    expect(stack).toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: [
              'firehose:PutRecord',
              'firehose:PutRecordBatch',
            ],
            Resource: stack.resolve(deliveryStream.deliveryStreamArn),
          },
        ],
      },
      Roles: [stack.resolve(role.roleName)],
    });
  });

  test('supplying 0 or multiple destinations throws', () => {
    expect(() => new firehose.DeliveryStream(stack, 'No Destinations', {
      destinations: [],
    })).toThrowError(/Only one destination is allowed per delivery stream/);
    expect(() => new firehose.DeliveryStream(stack, 'Too Many Destinations', {
      destinations: [mockS3Destination, mockS3Destination],
    })).toThrowError(/Only one destination is allowed per delivery stream/);
  });

  describe('metric methods provide a Metric with configured and attached properties', () => {
    beforeEach(() => {
      stack = new cdk.Stack(undefined, undefined, { env: { account: '111122223333', region: 'xx-west-1' } });
    });

    test('metric', () => {
      const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
        destinations: [mockS3Destination],
      });

      const metric = deliveryStream.metric('IncomingRecords');

      expect(metric).toMatchObject({
        account: stack.account,
        region: stack.region,
        namespace: 'AWS/Firehose',
        metricName: 'IncomingRecords',
        dimensions: {
          DeliveryStreamName: deliveryStream.deliveryStreamName,
        },
      });
    });

    test('metricIncomingBytes', () => {
      const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
        destinations: [mockS3Destination],
      });

      const metric = deliveryStream.metricIncomingBytes();

      expect(metric).toMatchObject({
        account: stack.account,
        region: stack.region,
        namespace: 'AWS/Firehose',
        metricName: 'IncomingBytes',
        statistic: cloudwatch.Statistic.AVERAGE,
        dimensions: {
          DeliveryStreamName: deliveryStream.deliveryStreamName,
        },
      });
    });

    test('metricIncomingRecords', () => {
      const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
        destinations: [mockS3Destination],
      });

      const metric = deliveryStream.metricIncomingRecords();

      expect(metric).toMatchObject({
        account: stack.account,
        region: stack.region,
        namespace: 'AWS/Firehose',
        metricName: 'IncomingRecords',
        statistic: cloudwatch.Statistic.AVERAGE,
        dimensions: {
          DeliveryStreamName: deliveryStream.deliveryStreamName,
        },
      });
    });

    test('metricBackupToS3Bytes', () => {
      const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
        destinations: [mockS3Destination],
      });

      const metric = deliveryStream.metricBackupToS3Bytes();

      expect(metric).toMatchObject({
        account: stack.account,
        region: stack.region,
        namespace: 'AWS/Firehose',
        metricName: 'BackupToS3.Bytes',
        statistic: cloudwatch.Statistic.AVERAGE,
        dimensions: {
          DeliveryStreamName: deliveryStream.deliveryStreamName,
        },
      });
    });

    test('metricBackupToS3DataFreshness', () => {
      const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
        destinations: [mockS3Destination],
      });

      const metric = deliveryStream.metricBackupToS3DataFreshness();

      expect(metric).toMatchObject({
        account: stack.account,
        region: stack.region,
        namespace: 'AWS/Firehose',
        metricName: 'BackupToS3.DataFreshness',
        statistic: cloudwatch.Statistic.AVERAGE,
        dimensions: {
          DeliveryStreamName: deliveryStream.deliveryStreamName,
        },
      });
    });

    test('metricBackupToS3Records', () => {
      const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
        destinations: [mockS3Destination],
      });

      const metric = deliveryStream.metricBackupToS3Records();

      expect(metric).toMatchObject({
        account: stack.account,
        region: stack.region,
        namespace: 'AWS/Firehose',
        metricName: 'BackupToS3.Records',
        statistic: cloudwatch.Statistic.AVERAGE,
        dimensions: {
          DeliveryStreamName: deliveryStream.deliveryStreamName,
        },
      });
    });
  });

  test('allows connections for Firehose IP addresses using map when region not specified', () => {
    const vpc = new ec2.Vpc(stack, 'VPC');
    const securityGroup = new ec2.SecurityGroup(stack, 'Security Group', { vpc });
    const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    securityGroup.connections.allowFrom(deliveryStream, ec2.Port.allTcp());

    expect(stack).toHaveResourceLike('AWS::EC2::SecurityGroup', {
      SecurityGroupIngress: [
        {
          CidrIp: {
            'Fn::FindInMap': [
              'DeliveryStreamFirehoseCIDRMappingE9233479',
              {
                Ref: 'AWS::Region',
              },
              'FirehoseCidrBlock',
            ],
          },
        },
      ],
    });
  });

  test('allows connections for Firehose IP addresses using literal when region specified', () => {
    stack = new cdk.Stack(undefined, undefined, { env: { region: 'us-west-1' } });
    const vpc = new ec2.Vpc(stack, 'VPC');
    const securityGroup = new ec2.SecurityGroup(stack, 'Security Group', { vpc });
    const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    securityGroup.connections.allowFrom(deliveryStream, ec2.Port.allTcp());

    expect(stack).toHaveResourceLike('AWS::EC2::SecurityGroup', {
      SecurityGroupIngress: [
        {
          CidrIp: '13.57.135.192/27',
        },
      ],
    });
  });

  test('can add tags', () => {
    const deliveryStream = new firehose.DeliveryStream(stack, 'Delivery Stream', {
      destinations: [mockS3Destination],
    });

    cdk.Tags.of(deliveryStream).add('tagKey', 'tagValue');

    expect(stack).toHaveResource('AWS::KinesisFirehose::DeliveryStream', {
      Tags: [
        {
          Key: 'tagKey',
          Value: 'tagValue',
        },
      ],
    });
  });

  describe('importing', () => {
    test('from name', () => {
      const deliveryStream = firehose.DeliveryStream.fromDeliveryStreamName(stack, 'DeliveryStream', 'mydeliverystream');

      expect(deliveryStream.deliveryStreamName).toBe('mydeliverystream');
      expect(stack.resolve(deliveryStream.deliveryStreamArn)).toStrictEqual({
        'Fn::Join': ['', ['arn:', stack.resolve(stack.partition), ':firehose:', stack.resolve(stack.region), ':', stack.resolve(stack.account), ':deliverystream/mydeliverystream']],
      });
      expect(deliveryStream.grantPrincipal).toBeInstanceOf(iam.UnknownPrincipal);
    });

    test('from ARN', () => {
      const deliveryStream = firehose.DeliveryStream.fromDeliveryStreamArn(stack, 'DeliveryStream', 'arn:aws:firehose:xx-west-1:111122223333:deliverystream/mydeliverystream');

      expect(deliveryStream.deliveryStreamName).toBe('mydeliverystream');
      expect(deliveryStream.deliveryStreamArn).toBe('arn:aws:firehose:xx-west-1:111122223333:deliverystream/mydeliverystream');
      expect(deliveryStream.grantPrincipal).toBeInstanceOf(iam.UnknownPrincipal);
    });

    test('from attributes (just name)', () => {
      const deliveryStream = firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'DeliveryStream', { deliveryStreamName: 'mydeliverystream' });

      expect(deliveryStream.deliveryStreamName).toBe('mydeliverystream');
      expect(stack.resolve(deliveryStream.deliveryStreamArn)).toStrictEqual({
        'Fn::Join': ['', ['arn:', stack.resolve(stack.partition), ':firehose:', stack.resolve(stack.region), ':', stack.resolve(stack.account), ':deliverystream/mydeliverystream']],
      });
      expect(deliveryStream.grantPrincipal).toBeInstanceOf(iam.UnknownPrincipal);
    });

    test('from attributes (just ARN)', () => {
      const deliveryStream = firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'DeliveryStream', { deliveryStreamArn: 'arn:aws:firehose:xx-west-1:111122223333:deliverystream/mydeliverystream' });

      expect(deliveryStream.deliveryStreamName).toBe('mydeliverystream');
      expect(deliveryStream.deliveryStreamArn).toBe('arn:aws:firehose:xx-west-1:111122223333:deliverystream/mydeliverystream');
      expect(deliveryStream.grantPrincipal).toBeInstanceOf(iam.UnknownPrincipal);
    });

    test('from attributes (with role)', () => {
      const role = iam.Role.fromRoleArn(stack, 'Delivery Stream Role', 'arn:aws:iam::111122223333:role/DeliveryStreamRole');
      const deliveryStream = firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'DeliveryStream', { deliveryStreamName: 'mydeliverystream', role });

      expect(deliveryStream.deliveryStreamName).toBe('mydeliverystream');
      expect(stack.resolve(deliveryStream.deliveryStreamArn)).toStrictEqual({
        'Fn::Join': ['', ['arn:', stack.resolve(stack.partition), ':firehose:', stack.resolve(stack.region), ':', stack.resolve(stack.account), ':deliverystream/mydeliverystream']],
      });
      expect(deliveryStream.grantPrincipal).toBe(role);
    });

    test('throws when malformatted ARN', () => {
      expect(() => firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'DeliveryStream', { deliveryStreamArn: 'arn:aws:firehose:xx-west-1:111122223333:deliverystream/' }))
        .toThrowError(/Could not import delivery stream from malformatted ARN/);
    });

    test('throws when without name or ARN', () => {
      expect(() => firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'DeliveryStream', {}))
        .toThrowError('Either deliveryStreamName or deliveryStreamArn must be provided in DeliveryStreamAttributes');
    });
  });
});
