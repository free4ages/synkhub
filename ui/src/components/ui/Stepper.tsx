import React from 'react';
import { cn } from '../../utils/cn';

export interface StepperStep {
  id: string;
  title: string;
  description?: string;
  icon?: React.ReactNode;
}

interface StepperProps {
  steps: StepperStep[];
  currentStep: number;
  onStepClick?: (stepIndex: number) => void;
  className?: string;
}

export const Stepper: React.FC<StepperProps> = ({
  steps,
  currentStep,
  onStepClick,
  className
}) => {
  return (
    <div className={cn('w-full', className)}>
      <div className="flex items-center justify-between">
        {steps.map((step, index) => {
          const isActive = index === currentStep;
          const isCompleted = index < currentStep;
          const isClickable = onStepClick && (isCompleted || index === currentStep);

          return (
            <div key={step.id} className="flex items-center flex-1">
              {/* Step Circle */}
              <div className="flex flex-col items-center">
                <button
                  onClick={() => isClickable && onStepClick(index)}
                  disabled={!isClickable}
                  className={cn(
                    'w-10 h-10 rounded-full flex items-center justify-center border-2 transition-all duration-200',
                    {
                      'bg-blue-500 border-blue-500 text-white': isActive,
                      'bg-green-500 border-green-500 text-white': isCompleted,
                      'bg-gray-100 border-gray-300 text-gray-500': !isActive && !isCompleted,
                      'cursor-pointer hover:bg-blue-50 hover:border-blue-300': isClickable && !isActive && !isCompleted,
                      'cursor-not-allowed': !isClickable
                    }
                  )}
                >
                  {isCompleted ? (
                    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                    </svg>
                  ) : step.icon ? (
                    step.icon
                  ) : (
                    <span className="text-sm font-medium">{index + 1}</span>
                  )}
                </button>
                
                {/* Step Title */}
                <div className="mt-2 text-center">
                  <div className={cn(
                    'text-sm font-medium',
                    {
                      'text-blue-600': isActive,
                      'text-green-600': isCompleted,
                      'text-gray-500': !isActive && !isCompleted
                    }
                  )}>
                    {step.title}
                  </div>
                  {step.description && (
                    <div className="text-xs text-gray-400 mt-1 max-w-24">
                      {step.description}
                    </div>
                  )}
                </div>
              </div>

              {/* Connector Line */}
              {index < steps.length - 1 && (
                <div className={cn(
                  'flex-1 h-0.5 mx-4 transition-colors duration-200',
                  {
                    'bg-green-500': isCompleted,
                    'bg-gray-300': !isCompleted
                  }
                )} />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
};

interface StepperFormProps {
  steps: StepperStep[];
  currentStep: number;
  onStepChange: (step: number) => void;
  onNext: () => void;
  onPrevious: () => void;
  onComplete?: () => void;
  canProceed: boolean;
  isLoading?: boolean;
  children: React.ReactNode;
  className?: string;
}

export const StepperForm: React.FC<StepperFormProps> = ({
  steps,
  currentStep,
  onStepChange,
  onNext,
  onPrevious,
  onComplete,
  canProceed,
  isLoading = false,
  children,
  className
}) => {
  const isLastStep = currentStep === steps.length - 1;
  const isFirstStep = currentStep === 0;

  return (
    <div className={cn('w-full max-w-4xl mx-auto', className)}>
      {/* Stepper Header */}
      <div className="mb-8">
        <Stepper
          steps={steps}
          currentStep={currentStep}
          onStepClick={onStepChange}
        />
      </div>

      {/* Step Content */}
      <div className="mb-6">
        {children}
      </div>

      {/* Navigation Buttons */}
      <div className="flex justify-between items-center">
        <button
          onClick={onPrevious}
          disabled={isFirstStep || isLoading}
          className={cn(
            'px-6 py-2 rounded-md font-medium transition-colors duration-200',
            {
              'bg-gray-500 text-white hover:bg-gray-600': !isFirstStep && !isLoading,
              'bg-gray-300 text-gray-500 cursor-not-allowed': isFirstStep || isLoading
            }
          )}
        >
          Previous
        </button>

        <div className="flex items-center space-x-2">
          <span className="text-sm text-gray-500">
            Step {currentStep + 1} of {steps.length}
          </span>
        </div>

        <button
          onClick={isLastStep ? onComplete : onNext}
          disabled={!canProceed || isLoading}
          className={cn(
            'px-6 py-2 rounded-md font-medium transition-colors duration-200',
            {
              'bg-blue-500 text-white hover:bg-blue-600': canProceed && !isLoading,
              'bg-gray-300 text-gray-500 cursor-not-allowed': !canProceed || isLoading
            }
          )}
        >
          {isLoading ? (
            <div className="flex items-center space-x-2">
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              <span>Loading...</span>
            </div>
          ) : isLastStep ? (
            'Complete'
          ) : (
            'Next'
          )}
        </button>
      </div>
    </div>
  );
};
