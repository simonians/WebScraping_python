import sys


def print_progress_bar(value, end_value, message="", start_value=0, bar_length=20):
    percent = (float(value) - float(start_value)) / (float(end_value) - float(start_value))
    arrow = '-' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("\r {}[{}] {:.2f}%".format(message, arrow + spaces, float(round(percent * 100, 2))))
