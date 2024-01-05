import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stats
def group_numbers_in_file(file_path):
   with open(file_path, 'r') as file:
       lines = file.readlines()


   # Convert lines to numbers
   numbers = [float(line.strip()) for line in lines]


   # Group numbers into lists of 10
   grouped_numbers = [numbers[i:i + 10] for i in range(0, len(numbers), 10)]


   return grouped_numbers
def plot_lists_with_stats(lists_of_numbers, list_names):
   for i, numbers in enumerate(lists_of_numbers):
       # Calculate statistics
       max_value = max(numbers)
       min_value = min(numbers)
       average = np.mean(numbers)


       # Confidence interval (95%)
       ci = stats.sem(numbers) * stats.t.ppf((1 + 0.95) / 2, len(numbers) - 1)


       # Plotting
       plt.errorbar(i, average, yerr=ci, fmt='o', label=f'{list_names[i]} (Max: {max_value}, Min: {min_value})')


   plt.xticks(range(len(lists_of_numbers)), list_names, rotation=45)
   plt.ylabel('Value')
   plt.title('List Values with 95% Confidence Interval')
   plt.legend()
   plt.show()


# Example usage
list_of_lists = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [2, 3, 4, 5, 6, 7, 8, 9, 10, 11], ...]
list_of_names = ['List1', 'List2', ...]
plot_lists_with_stats(list_of_lists, list_of_names)
