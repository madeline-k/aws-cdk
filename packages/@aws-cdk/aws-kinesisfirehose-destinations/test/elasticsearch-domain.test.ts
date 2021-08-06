import { Match, Template } from '@aws-cdk/assertions';
import * as es from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as kms from '@aws-cdk/aws-kms';
import * as lambda from '@aws-cdk/aws-lambda';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import * as firehosedestinations from '../lib';

describe('Elasticsearch destination', () => {
  let stack: cdk.Stack;
  let domain: es.IDomain;
  let destinationRole: iam.IRole;

  beforeEach(() => {
    stack = new cdk.Stack();
    domain = new es.Domain(stack, 'domain', {
      version: es.ElasticsearchVersion.V7_9,
    });
    destinationRole = new iam.Role(stack, 'Destination Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });
  });

  it('provides defaults when no configuration is provided', () => {
    new firehose.DeliveryStream(stack, 'DeliveryStream', {
      destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
      })],
    });

    Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
      ElasticsearchDestinationConfiguration: {
        DomainARN: stack.resolve(domain.domainArn),
        CloudWatchLoggingOptions: {
          Enabled: true,
        },
        S3BackupMode: 'FailedDocumentsOnly',
        S3Configuration: {
          BucketARN: {
            'Fn::GetAtt': [
              'DeliveryStreamBackupBucket48C8465F',
              'Arn',
            ],
          },
          RoleARN: stack.resolve(destinationRole.roleArn),
          CloudWatchLoggingOptions: {
            Enabled: true,
          },
        },
        RoleARN: stack.resolve(destinationRole.roleArn),
      },
    });
    Template.fromStack(stack).resourceCountIs('AWS::Logs::LogGroup', 1);
    Template.fromStack(stack).resourceCountIs('AWS::Logs::LogStream', 2);
  });

  it('creates a role when none is provided', () => {
    new firehose.DeliveryStream(stack, 'DeliveryStream', {
      destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
      })],
    });

    Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
      ElasticsearchDestinationConfiguration: {
        RoleARN: {
          'Fn::GetAtt': [
            'DestinationRole715116B2',
            'Arn',
          ],
        },
      },
    });
    Template.fromStack(stack).hasResourceProperties('AWS::IAM::Role', {
      AssumeRolePolicyDocument: {
        Statement: [
          {
            Effect: 'Allow',
            Principal: {
              Service: 'firehose.amazonaws.com',
            },
            Action: 'sts:AssumeRole',
          },
        ],
      },
    });
  });

  it('grants read/write access to the domain', () => {
    const destination = new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: 'my_index',
      role: destinationRole,
    });

    new firehose.DeliveryStream(stack, 'DeliveryStream', {
      destinations: [destination],
    });

    Template.fromStack(stack).hasResourceProperties('AWS::IAM::Policy', {
      Roles: [stack.resolve(destinationRole.roleName)],
      PolicyDocument: {
        Statement: Match.arrayWith([
          {
            Action: [
              'es:ESHttpGet',
              'es:ESHttpHead',
              'es:ESHttpDelete',
              'es:ESHttpPost',
              'es:ESHttpPut',
              'es:ESHttpPatch',
            ],
            Effect: 'Allow',
            Resource: [
              stack.resolve(domain.domainArn),
              { 'Fn::Join': ['', [stack.resolve(domain.domainArn), '/*']] },
            ],
          },
          {
            Action: [
              'es:DescribeElasticsearchDomain',
              'es:DescribeElasticsearchDomains',
              'es:DescribeElasticsearchDomainConfig',
            ],
            Effect: 'Allow',
            Resource: [
              stack.resolve(domain.domainArn),
              { 'Fn::Join': ['', [stack.resolve(domain.domainArn), '/*']] },
            ],
          },
        ]),
      },
    });
  });

  it('domain and log group grants are depended on by delivery stream', () => {
    const logGroup = logs.LogGroup.fromLogGroupName(stack, 'Log Group', 'evergreen');
    const destination = new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: 'my_index',
      role: destinationRole,
      logGroup: logGroup,
    });
    new firehose.DeliveryStream(stack, 'DeliveryStream', {
      destinations: [destination],
    });

    Template.fromStack(stack).hasResourceProperties('AWS::IAM::Policy', {
      PolicyName: 'DestinationRoleDefaultPolicy1185C75D',
      Roles: [stack.resolve(destinationRole.roleName)],
      PolicyDocument: {
        Statement: Match.arrayWith([
          {
            Action: [
              'es:ESHttpGet',
              'es:ESHttpHead',
              'es:ESHttpDelete',
              'es:ESHttpPost',
              'es:ESHttpPut',
              'es:ESHttpPatch',
            ],
            Effect: 'Allow',
            Resource: [
              stack.resolve(domain.domainArn),
              { 'Fn::Join': ['', [stack.resolve(domain.domainArn), '/*']] },
            ],
          },
          {
            Action: [
              'es:DescribeElasticsearchDomain',
              'es:DescribeElasticsearchDomains',
              'es:DescribeElasticsearchDomainConfig',
            ],
            Effect: 'Allow',
            Resource: [
              stack.resolve(domain.domainArn),
              { 'Fn::Join': ['', [stack.resolve(domain.domainArn), '/*']] },
            ],
          },
          {
            Action: [
              'logs:CreateLogStream',
              'logs:PutLogEvents',
            ],
            Effect: 'Allow',
            Resource: stack.resolve(logGroup.logGroupArn),
          },
        ]),
      },
    });
    Template.fromStack(stack).hasResource('AWS::KinesisFirehose::DeliveryStream', {
      DependsOn: ['DestinationRoleDefaultPolicy1185C75D'],
    });
  });

  it('throws with invalid indexName', () => {
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: ',',
    })).toThrowError('Elasticsearch index name must not contain commas. indexName provided: ,');
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: '_index',
    })).toThrowError('Elasticsearch index name must not begin with an underscore. indexName provided: _index');
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: 'MyIndex',
    })).toThrowError('Elasticsearch index name must be lower-case. indexName provided: MyIndex');
  });

  it('sets retry options', () => {
    new firehose.DeliveryStream(stack, 'DeliveryStream', {
      destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        retryInterval: cdk.Duration.seconds(120),
      })],
    });

    Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
      ElasticsearchDestinationConfiguration: {
        RetryOptions: {
          DurationInSeconds: 120,
        },
      },
    });
  });

  it('throws with invalid retryInterval', () => {
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: 'my_index',
      retryInterval: cdk.Duration.hours(3),
    })).toThrowError('retry interval too big');
  });

  it('sets index rotation period', () => {
    new firehose.DeliveryStream(stack, 'DeliveryStream', {
      destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        indexRotation: firehosedestinations.IndexRotationPeriod.ONE_MONTH,
      })],
    });

    Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
      ElasticsearchDestinationConfiguration: {
        IndexRotationPeriod: 'OneMonth',
      },
    });
  });

  describe('logging', () => {
    it('creates resources and configuration by default', () => {
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          role: destinationRole,
        })],
      });

      Template.fromStack(stack).resourceCountIs('AWS::Logs::LogGroup', 1);
      Template.fromStack(stack).resourceCountIs('AWS::Logs::LogStream', 2);
      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          CloudWatchLoggingOptions: {
            Enabled: true,
          },
          S3Configuration: {
            CloudWatchLoggingOptions: {
              Enabled: true,
            },
          },
        },
      });
    });

    it('does not create resources or configuration if disabled', () => {
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          role: destinationRole,
          logging: false,
          s3Backup: {
            logging: false,
          },
        })],
      });

      Template.fromStack(stack).resourceCountIs('AWS::Logs::LogGroup', 0);
      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          CloudWatchLoggingOptions: Match.absentProperty(),
          S3Configuration: {
            CloudWatchLoggingOptions: Match.absentProperty(),
          },
        },
      });
    });

    it('uses provided log group', () => {
      const logGroup = new logs.LogGroup(stack, 'Log Group');
      const backupLogGroup = new logs.LogGroup(stack, 'BackupLogGroup');

      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          role: destinationRole,
          logGroup: logGroup,
          s3Backup: {
            logGroup: backupLogGroup,
          },
        })],
      });

      Template.fromStack(stack).resourceCountIs('AWS::Logs::LogGroup', 2);
      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          CloudWatchLoggingOptions: {
            Enabled: true,
            LogGroupName: stack.resolve(logGroup.logGroupName),
          },
          S3Configuration: {
            CloudWatchLoggingOptions: {
              Enabled: true,
              LogGroupName: stack.resolve(backupLogGroup.logGroupName),
            },
          },
        },
      });
    });

    it('throws error if logging disabled but log group provided', () => {
      const destination = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        logging: false,
        logGroup: new logs.LogGroup(stack, 'LogGroup'),
      });
      expect(() => new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destination],
      })).toThrowError('logging cannot be set to false when logGroup is provided');
    });

    it('grants log group write permissions to destination role', () => {
      const logGroup = new logs.LogGroup(stack, 'Log Group');

      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          role: destinationRole,
          logGroup: logGroup,
        })],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::IAM::Policy', {
        Roles: [stack.resolve(destinationRole.roleName)],
        PolicyDocument: {
          Statement: Match.arrayWith([
            {
              Action: [
                'logs:CreateLogStream',
                'logs:PutLogEvents',
              ],
              Effect: 'Allow',
              Resource: stack.resolve(logGroup.logGroupArn),
            },
          ]),
        },
      });
    });
  });

  describe('processing configuration', () => {
    let lambdaFunction: lambda.IFunction;
    let basicLambdaProcessor: firehose.LambdaFunctionProcessor;
    let destinationWithBasicLambdaProcessor: firehosedestinations.ElasticsearchDomain;

    beforeEach(() => {
      lambdaFunction = new lambda.Function(stack, 'DataProcessorFunction', {
        runtime: lambda.Runtime.NODEJS_12_X,
        code: lambda.Code.fromInline('foo'),
        handler: 'bar',
      });
      basicLambdaProcessor = new firehose.LambdaFunctionProcessor(lambdaFunction);
      destinationWithBasicLambdaProcessor = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        processor: basicLambdaProcessor,
      });
    });

    it('creates configuration for LambdaFunctionProcessor', () => {
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destinationWithBasicLambdaProcessor],
      });

      Template.fromStack(stack).resourceCountIs('AWS::Lambda::Function', 1);
      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          ProcessingConfiguration: {
            Enabled: true,
            Processors: [{
              Type: 'Lambda',
              Parameters: [
                {
                  ParameterName: 'RoleArn',
                  ParameterValue: stack.resolve(destinationRole.roleArn),
                },
                {
                  ParameterName: 'LambdaArn',
                  ParameterValue: stack.resolve(lambdaFunction.functionArn),
                },
              ],
            }],
          },
        },
      });
    });

    it('set all optional parameters', () => {
      const processor = new firehose.LambdaFunctionProcessor(lambdaFunction, {
        bufferInterval: cdk.Duration.minutes(1),
        bufferSize: cdk.Size.mebibytes(1),
        retries: 5,
      });
      const destination = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        processor: processor,
      });
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destination],
      });

      Template.fromStack(stack).resourceCountIs('AWS::Lambda::Function', 1);
      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          ProcessingConfiguration: {
            Enabled: true,
            Processors: [{
              Type: 'Lambda',
              Parameters: [
                {
                  ParameterName: 'RoleArn',
                  ParameterValue: stack.resolve(destinationRole.roleArn),
                },
                {
                  ParameterName: 'LambdaArn',
                  ParameterValue: stack.resolve(lambdaFunction.functionArn),
                },
                {
                  ParameterName: 'BufferIntervalInSeconds',
                  ParameterValue: '60',
                },
                {
                  ParameterName: 'BufferSizeInMBs',
                  ParameterValue: '1',
                },
                {
                  ParameterName: 'NumberOfRetries',
                  ParameterValue: '5',
                },
              ],
            }],
          },
        },
      });
    });

    it('grants invoke access to the lambda function and delivery stream depends on grant', () => {
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destinationWithBasicLambdaProcessor],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::IAM::Policy', {
        PolicyName: 'DestinationRoleDefaultPolicy1185C75D',
        Roles: [stack.resolve(destinationRole.roleName)],
        PolicyDocument: {
          Statement: Match.arrayWith([
            {
              Action: 'lambda:InvokeFunction',
              Effect: 'Allow',
              Resource: stack.resolve(lambdaFunction.functionArn),
            },
          ]),
        },
      });
      Template.fromStack(stack).hasResource('AWS::KinesisFirehose::DeliveryStream', {
        DependsOn: ['DestinationRoleDefaultPolicy1185C75D'],
      });
    });
  });

  describe('buffering', () => {
    it('creates configuration when interval and size provided', () => {
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          bufferingInterval: cdk.Duration.minutes(1),
          bufferingSize: cdk.Size.mebibytes(1),
        })],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          BufferingHints: {
            IntervalInSeconds: 60,
            SizeInMBs: 1,
          },
        },
      });
    });

    it('validates bufferingInterval', () => {
      expect(() => new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          bufferingInterval: cdk.Duration.seconds(30),
          bufferingSize: cdk.Size.mebibytes(1),
        })],
      })).toThrowError('Buffering interval must be between 60 and 900 seconds. Buffering interval provided was 30 seconds.');

      expect(() => new firehose.DeliveryStream(stack, 'DeliveryStream2', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          bufferingInterval: cdk.Duration.minutes(16),
          bufferingSize: cdk.Size.mebibytes(1),
        })],
      })).toThrowError('Buffering interval must be between 60 and 900 seconds. Buffering interval provided was 960 seconds.');
    });

    it('validates bufferingSize', () => {
      expect(() => new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          bufferingInterval: cdk.Duration.minutes(1),
          bufferingSize: cdk.Size.mebibytes(0),
        })],
      })).toThrowError('Buffering size must be between 1 and 128 MiBs. Buffering size provided was 0 MiBs');

      expect(() => new firehose.DeliveryStream(stack, 'DeliveryStream2', {
        destinations: [new firehosedestinations.ElasticsearchDomain(domain, {
          indexName: 'my_index',
          bufferingInterval: cdk.Duration.minutes(1),
          bufferingSize: cdk.Size.mebibytes(256),
        })],
      })).toThrowError('Buffering size must be between 1 and 128 MiBs. Buffering size provided was 256 MiBs');
    });
  });

  describe('s3 backup configuration', () => {
    it('backup resources created by default', () => {
      const destination = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
      });
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destination],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          S3Configuration: {
            CloudWatchLoggingOptions: {
              Enabled: true,
            },
            RoleARN: stack.resolve(destinationRole.roleArn),
          },
          S3BackupMode: 'FailedDocumentsOnly',
        },
      });
    });

    it('sets backup configuration if backup bucket provided', () => {
      const backupBucket = new s3.Bucket(stack, 'MyBackupBucket');
      const destination = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        s3Backup: {
          bucket: backupBucket,
        },
      });
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destination],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          S3Configuration: {
            BucketARN: stack.resolve(backupBucket.bucketArn),
            CloudWatchLoggingOptions: {
              Enabled: true,
            },
            RoleARN: stack.resolve(destinationRole.roleArn),
          },
          S3BackupMode: 'FailedDocumentsOnly',
        },
      });
    });

    it('backup mode set to ALL', () => {
      const destination = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        s3Backup: {
          mode: firehosedestinations.BackupMode.ALL,
        },
      });
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destination],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          S3Configuration: {
            CloudWatchLoggingOptions: {
              Enabled: true,
            },
            RoleARN: stack.resolve(destinationRole.roleArn),
          },
          S3BackupMode: 'AllDocuments',
        },
      });
    });

    it('sets full backup configuration', () => {
      const backupBucket = new s3.Bucket(stack, 'MyBackupBucket');
      const key = new kms.Key(stack, 'Key');
      const logGroup = new logs.LogGroup(stack, 'BackupLogGroup');
      const destination = new firehosedestinations.ElasticsearchDomain(domain, {
        indexName: 'my_index',
        role: destinationRole,
        s3Backup: {
          mode: firehosedestinations.BackupMode.ALL,
          bucket: backupBucket,
          dataOutputPrefix: 'myBackupPrefix',
          errorOutputPrefix: 'myBackupErrorPrefix',
          bufferingSize: cdk.Size.mebibytes(1),
          bufferingInterval: cdk.Duration.minutes(1),
          compression: firehosedestinations.Compression.ZIP,
          encryptionKey: key,
          logging: true,
          logGroup: logGroup,
        },
      });
      new firehose.DeliveryStream(stack, 'DeliveryStream', {
        destinations: [destination],
      });

      Template.fromStack(stack).hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
        ElasticsearchDestinationConfiguration: {
          S3Configuration: {
            BucketARN: stack.resolve(backupBucket.bucketArn),
            CloudWatchLoggingOptions: {
              Enabled: true,
              LogGroupName: stack.resolve(logGroup.logGroupName),
            },
            RoleARN: stack.resolve(destinationRole.roleArn),
            EncryptionConfiguration: {
              KMSEncryptionConfig: {
                AWSKMSKeyARN: stack.resolve(key.keyArn),
              },
            },
            Prefix: 'myBackupPrefix',
            ErrorOutputPrefix: 'myBackupErrorPrefix',
            BufferingHints: {
              IntervalInSeconds: 60,
              SizeInMBs: 1,
            },
            CompressionFormat: 'ZIP',
          },
          S3BackupMode: 'AllDocuments',
        },
      });
    });
  });
});