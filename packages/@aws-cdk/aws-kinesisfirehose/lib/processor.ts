import { Duration, Size } from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import { IDeliveryStream } from './delivery-stream';

/**
 * Configure the data processor.
 */
export interface DataProcessorProps {

  /**
   * The length of time Firehose will buffer incoming data before calling the processor.
   *
   * @default Duration.minutes(1)
   */
  readonly bufferInterval?: Duration;

  /**
   * The amount of incoming data Firehose will buffer before calling the processor.
   *
   * @default Size.mebibytes(3)
   */
  readonly bufferSize?: Size;

  /**
   * The number of times Firehose will retry the Lambda function invocation due to network timeout or invocation limits.
   *
   * @default 3
   */
  readonly retries?: number;
}

export interface DataProcessorIdentifier {
  readonly parameterName: string;
  readonly parameterValue: string;
}

export interface DataProcessorConfig extends DataProcessorProps {
  readonly processorType: string;
  readonly processorIdentifier: DataProcessorIdentifier;
}

/**
 * A data processor that Firehose will call to transform records before delivering data.
 */
export abstract class DataProcessor {
  public abstract bind(deliveryStream: IDeliveryStream): DataProcessorConfig
}

/**
 * Use a Lambda function to transform records.
 * TODO: inspect timeout to validate < 5 minutes?
 */
export class LambdaFunctionProcessor extends DataProcessor {
  private readonly processorType = 'Lambda';
  private readonly processorIdentifier: DataProcessorIdentifier;
  constructor(private readonly lambdaFunction: lambda.IFunction, private readonly props: DataProcessorProps = {}) {
    super();

    this.processorIdentifier = {
      parameterName: 'LambdaArn',
      parameterValue: lambdaFunction.functionArn,
    };
  }

  public bind(deliveryStream: IDeliveryStream): DataProcessorConfig {
    this.lambdaFunction.grantInvoke(deliveryStream);

    return {
      processorType: this.processorType,
      processorIdentifier: this.processorIdentifier,
      ...this.props,
    };
  }
}
