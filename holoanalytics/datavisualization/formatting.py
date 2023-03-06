import numpy as np
from holoanalytics.utils import calculation as calc


def set_upper_limits(ax, max_x=None, max_y=None):

    if isinstance(max_x, (int, np.integer, float, np.float)):
        interval = calc.calculate_interval(max_x)
        x_limit = calc.round_up_nearest_x(max_x, interval)
        ax.set_xlim(right=x_limit)

    if isinstance(max_y, (int, np.integer, float, np.float)):
        interval = calc.calculate_interval(max_y)
        y_limit = calc.round_up_nearest_x(max_y, interval)
        ax.set_ylim(top=y_limit)

