import '@aws-cdk/assert-internal/jest';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { Construct } from 'constructs';
import * as firehose from '../lib';
import { LambdaFunctionProcessor } from '../lib/processor';

describe('destination', () => {
  let stack: cdk.Stack;
  let deliveryStreamRole: iam.IRole;
  let deliveryStream: firehose.IDeliveryStream;

  beforeEach(() => {
    stack = new cdk.Stack();
    deliveryStreamRole = iam.Role.fromRoleArn(stack, 'Delivery Stream Role', 'arn:aws:iam::111122223333:role/DeliveryStreamRole');
    deliveryStream = firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'Delivery Stream', {
      deliveryStreamName: 'mydeliverystream',
      role: deliveryStreamRole,
    });
  });

  describe('createLoggingOptions', () => {
    class LoggingDestination extends firehose.DestinationBase {
      public bind(scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
        return {
          properties: {
            testDestinationConfig: {
              loggingConfig: this.createLoggingOptions(scope, options.deliveryStream, 'streamId'),
            },
          },
        };
      }
    }

    test('creates resources and configuration by default', () => {
      const testDestination = new LoggingDestination();

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack).toHaveResource('AWS::Logs::LogGroup');
      expect(stack).toHaveResource('AWS::Logs::LogStream');
      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            loggingConfig: {
              enabled: true,
              logGroupName: {
                Ref: 'LogGroupD9735569',
              },
              logStreamName: {
                Ref: 'LogGroupstreamIdA1293DC2',
              },
            },
          },
        },
      });
    });
    test('does not create resources or configuration if disabled', () => {
      const testDestination = new LoggingDestination({ logging: false });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {},
        },
      });
    });

    test('creates configuration if log group provided', () => {
      const testDestination = new LoggingDestination({ logGroup: new logs.LogGroup(stack, 'Log Group') });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toMatchObject({
        properties: {
          testDestinationConfig: {
            loggingConfig: {
              enabled: true,
            },
          },
        },
      });
    });

    test('throws error if logging disabled but log group provided', () => {
      const testDestination = new LoggingDestination({ logging: false, logGroup: new logs.LogGroup(stack, 'Log Group') });

      expect(() => testDestination.bind(stack, { deliveryStream })).toThrowError('Destination logging cannot be set to false when logGroup is provided');
    });

    test('uses provided log group', () => {
      const testDestination = new LoggingDestination({ logGroup: new logs.LogGroup(stack, 'Log Group') });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack).toCountResources('AWS::Logs::LogGroup', 1);
      expect(stack.resolve(testDestinationConfig)).toMatchObject({
        properties: {
          testDestinationConfig: {
            loggingConfig: {
              enabled: true,
              logGroupName: {
                Ref: 'LogGroupD9735569',
              },
              logStreamName: {
                Ref: 'LogGroupstreamIdA1293DC2',
              },
            },
          },
        },
      });
    });

    test('re-uses log group if called multiple times', () => {
      const testDestination = new class extends firehose.DestinationBase {
        public bind(scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
          return {
            properties: {
              testDestinationConfig: {
                loggingConfig: this.createLoggingOptions(scope, options.deliveryStream, 'streamId'),
                anotherLoggingConfig: this.createLoggingOptions(scope, options.deliveryStream, 'anotherStreamId'),
              },
            },
          };
        }
      }();

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack).toCountResources('AWS::Logs::LogGroup', 1);
      expect(stack.resolve(testDestinationConfig)).toMatchObject({
        properties: {
          testDestinationConfig: {
            loggingConfig: {
              logGroupName: {
                Ref: 'LogGroupD9735569',
              },
              logStreamName: {
                Ref: 'LogGroupstreamIdA1293DC2',
              },
            },
            anotherLoggingConfig: {
              logGroupName: {
                Ref: 'LogGroupD9735569',
              },
              logStreamName: {
                Ref: 'LogGroupanotherStreamIdE609928E',
              },
            },
          },
        },
      });
    });
  });

  describe('createBackupConfig', () => {
    class BackupDestination extends firehose.DestinationBase {
      public bind(scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
        return {
          properties: {
            testDestinationConfig: {
              backupConfig: this.createBackupConfig(scope, options.deliveryStream),
            },
          },
        };
      }
    }

    test('does not create resources or configuration by default', () => {
      const testDestination = new BackupDestination();

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {},
        },
      });
    });

    test('create resources and configuration if explicitly enabled', () => {
      const testDestination = new BackupDestination({ backup: firehose.BackupMode.ALL });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            backupConfig: {
              bucketArn: { 'Fn::GetAtt': ['BackupBucket1DE570B5', 'Arn'] },
              roleArn: 'arn:aws:iam::111122223333:role/DeliveryStreamRole',
            },
          },
        },
      });
    });

    test('creates configuration using bucket if provided', () => {
      const testDestination = new BackupDestination({ backupBucket: new s3.Bucket(stack, 'Bucket') });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack).toCountResources('AWS::S3::Bucket', 1);
      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            backupConfig: {
              bucketArn: { 'Fn::GetAtt': ['Bucket83908E77', 'Arn'] },
              roleArn: 'arn:aws:iam::111122223333:role/DeliveryStreamRole',
            },
          },
        },
      });
    });

    test('throws error if backup disabled and bucket provided', () => {
      const testDestination = new BackupDestination({ backup: firehose.BackupMode.DISABLED, backupBucket: new s3.Bucket(stack, 'Bucket') });

      expect(() => testDestination.bind(stack, { deliveryStream })).toThrowError('Destination backup cannot be set to DISABLED when backupBucket is provided');
    });

    test('can configure backup prefix', () => {
      const testDestination = new BackupDestination({ backup: firehose.BackupMode.ALL, backupPrefix: 'backupPrefix' });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toMatchObject({
        properties: {
          testDestinationConfig: {
            backupConfig: {
              prefix: 'backupPrefix',
            },
          },
        },
      });
    });
  });

  describe('createProcessingConfig', () => {
    class ProcessingDestination extends firehose.DestinationBase {
      public bind(_scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
        return {
          properties: {
            testDestinationConfig: {
              processingConfig: this.createProcessingConfig(options.deliveryStream),
            },
          },
        };
      }
    }

    let lambdaFunction: lambda.IFunction;
    beforeEach(() => {
      lambdaFunction = lambda.Function.fromFunctionAttributes(stack, 'Processor', {
        functionArn: 'arn:aws:lambda:xx-west-1:111122223333:function:my-function',
        sameEnvironment: true,
      });
    });

    test('does not create configuration by default', () => {
      const testDestination = new ProcessingDestination();

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {},
        },
      });
    });

    test('does not create configuration if processors array is empty', () => {
      const testDestination = new ProcessingDestination({ processors: [] });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {},
        },
      });
    });

    test('creates configuration if a processor is specified with only required parameters', () => {
      const testDestination = new ProcessingDestination({ processors: [new LambdaFunctionProcessor(lambdaFunction, {})] });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            processingConfig: {
              enabled: true,
              processors: [
                {
                  parameters: [
                    {
                      parameterName: 'RoleArn',
                      parameterValue: 'arn:aws:iam::111122223333:role/DeliveryStreamRole',
                    },
                    {
                      parameterName: 'LambdaArn',
                      parameterValue: 'arn:aws:lambda:xx-west-1:111122223333:function:my-function',
                    },
                  ],
                  type: 'Lambda',
                },
              ],
            },
          },
        },
      });
    });

    test('creates configuration if a processor is specified with optional parameters', () => {
      const testDestination = new ProcessingDestination({
        processors: [
          new LambdaFunctionProcessor(lambdaFunction, { bufferInterval: cdk.Duration.minutes(1), bufferSize: cdk.Size.kibibytes(1024), retries: 1 }),
        ],
      });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            processingConfig: {
              enabled: true,
              processors: [
                {
                  parameters: [
                    {
                      parameterName: 'RoleArn',
                      parameterValue: 'arn:aws:iam::111122223333:role/DeliveryStreamRole',
                    },
                    {
                      parameterName: 'LambdaArn',
                      parameterValue: 'arn:aws:lambda:xx-west-1:111122223333:function:my-function',
                    },
                    {
                      parameterName: 'BufferIntervalInSeconds',
                      parameterValue: '60',
                    },
                    {
                      parameterName: 'BufferSizeInMBs',
                      parameterValue: '1',
                    },
                    {
                      parameterName: 'NumberOfRetries',
                      parameterValue: '1',
                    },
                  ],
                  type: 'Lambda',
                },
              ],
            },
          },
        },
      });
    });

    test('throws an error if multiple processors are specified', () => {
      const testDestination = new ProcessingDestination({
        processors: [new LambdaFunctionProcessor(lambdaFunction), new LambdaFunctionProcessor(lambdaFunction)],
      });

      expect(() => testDestination.bind(stack, { deliveryStream })).toThrowError('Only one processor is allowed per delivery stream destination');
    });
  });

  describe('createBufferingHints', () => {
    class BufferingDestination extends firehose.DestinationBase {
      constructor(protected readonly props: { bufferingInterval?: cdk.Duration, bufferingSize?: cdk.Size } & firehose.DestinationProps = {}) {
        super(props);
      }

      public bind(_scope: Construct, _options: firehose.DestinationBindOptions): firehose.DestinationConfig {
        return {
          properties: {
            testDestinationConfig: {
              bufferingConfig: this.createBufferingHints(this.props.bufferingInterval, this.props.bufferingSize),
            },
          },
        };
      }
    }

    test('does not create configuration by default', () => {
      const testDestination = new BufferingDestination();

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {},
        },
      });
    });

    test('creates configuration when interval provided', () => {
      const testDestination = new BufferingDestination({ bufferingInterval: cdk.Duration.minutes(1) });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            bufferingConfig: {
              intervalInSeconds: 60,
            },
          },
        },
      });
    });

    test('creates configuration when size provided', () => {
      const testDestination = new BufferingDestination({ bufferingSize: cdk.Size.kibibytes(1024) });

      const testDestinationConfig = testDestination.bind(stack, { deliveryStream });

      expect(stack.resolve(testDestinationConfig)).toStrictEqual({
        properties: {
          testDestinationConfig: {
            bufferingConfig: {
              sizeInMBs: 1,
            },
          },
        },
      });
    });

    test('validates bufferingInterval', () => {
      expect(() => new BufferingDestination({ bufferingInterval: cdk.Duration.seconds(30) }).bind(stack, { deliveryStream }))
        .toThrowError('Buffering interval must be between 1 and 15 minutes');
      expect(() => new BufferingDestination({ bufferingInterval: cdk.Duration.minutes(16) }).bind(stack, { deliveryStream }))
        .toThrowError('Buffering interval must be between 1 and 15 minutes');
    });

    test('validates bufferingSize', () => {
      expect(() => new BufferingDestination({ bufferingSize: cdk.Size.mebibytes(0) }).bind(stack, { deliveryStream }))
        .toThrowError('Buffering size must be between 1 and 128 MBs');
      expect(() => new BufferingDestination({ bufferingSize: cdk.Size.mebibytes(256) }).bind(stack, { deliveryStream }))
        .toThrowError('Buffering size must be between 1 and 128 MBs');
    });
  });
});
