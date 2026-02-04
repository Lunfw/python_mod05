from typing import Any, List, Optional
from abc import ABC, abstractmethod
from sys import stderr


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        '''
            #   Main process function.
        '''
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        '''
            #   Main validation function.
        '''
        pass

    def format_output(self, result: str) -> str:
        '''
            #   Formats the result to an output.
        '''
        return ('Output: ' + result)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        '''
            #   Main validation function.
        '''
        try:
            for i in data:
                int(i)
        except ValueError:
            return False
        return True

    def process(self, data: Any) -> str:
        '''
            #   Attempts to process the current data, whether is is
            #   digit-based or not.
        '''
        diagnosis: str = 'Numeric data verified'
        output: str = ''

        print('Initializing Numeric Processor...')
        print(f'Processing data: {data}')
        if (not (self.validate(data))):
            diagnosis = 'Invalid data'
        print(f'Validation: {diagnosis}')
        try:
            output = f'Processed {len(data)} numeric values, '
            output += f'sum={sum(data)}, '
            output += f'avg={sum(data) / len(data)}\n'
            print(output)
        except (ZeroDivisionError, TypeError):
            output = '[ALERT] One non_digit detected\n'
            print(output)
        return (output)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        '''
            #   Main validation function.
        '''
        try:
            for i in data:
                str(i)
        except ValueError:
            return False
        return True

    def process(self, data: Any) -> str:
        '''
            #   Attempts to process the current data, whether is is
            #   text-based or not.
        '''
        diagnosis: str = 'Text data verified'
        output: str = ''

        print('Initializing Text Processor...')
        print(f'Processing data: {data}')
        if (not self.validate(data)):
            diagnosis = 'Invalid data'
        print(f'Validation: {diagnosis}')
        output = f'Processed text: {len(data)} characters, '
        output += f'{len(data.split())} words\n'
        print(super().format_output(output))
        return (output)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        '''
            #   Main validation function.
        '''
        can_close: bool = True
        try:
            with open(data, 'r') as file:
                file.read()
        except FileNotFoundError:
            can_close = False
            return
        if (can_close):
            file.close()
        return (True)

    def process(self, data: Any) -> str:
        '''
            #   Attempts to process a file.
        '''
        diagnosis: str = 'Log entry verified'
        output: str = ''

        print('Initializing Log Processor...')
        print(f'Processing data: {data}')
        if (not self.validate(data)):
            diagnosis = 'Could not verify log entry'
        print(f'Validation: {diagnosis}')
        try:
            open(data, 'r')
            output += '[INFO] INFO level detected: System ready\n'
            print(super().format_output(output))
        except Exception:
            output += '[ALERT] ERROR level detected: Could not read log\n'
            print(super().format_output(output), file=stderr)
        return (output)


def polymorphic_demo(processes: (list | tuple) = None) -> bool:
    '''
        #   Prints the results of all processes.
    '''
    error_check: bool = False
    print('=== Polymorphic Processing Demo ===\n')
    print('Processing multiple data type through same interface...')
    for i in range(len(processes)):
        print(f'Result {i + 1}: {processes[i]}', end='')
        if ('[ALERT]' in processes[i]):
            error_check = True
    return (error_check)


def numeric_lib() -> None:
    '''
        #   Small number generator
    '''
    for i in range(1, 6):
        yield i


def main_exec() -> None:
    '''
        #   Main execution program.
        #   Runs mandatory tests. (Example)
    '''
    temp: List[Any or Optional] = list(numeric_lib())
    text: str = 'Hello Nexus World'
    processes: list = []
    processes.append(NumericProcessor().process(temp))
    processes.append(TextProcessor().process(text))
    processes.append(LogProcessor().process('log_file.txt'))
    return (polymorphic_demo(processes))


def main() -> None:
    '''
        #   Small main program.
    '''
    print('=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n')
    if (not (main_exec())):
        print('\nFoundation systems online, Nexus ready for advanced streams.')
        return
    print('\nFoundation systems aren\'t optimal, but ready for streaming.')


if (__name__ == '__main__'):
    main()
