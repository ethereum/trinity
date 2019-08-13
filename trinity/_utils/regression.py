from typing import Tuple


def linear_regression(points: Tuple[Tuple[float, float], ...]) -> Tuple[float, float]:
    """
    :return: the slope and intercept of the linear regression

    Not convinced? Check:
    https://www.khanacademy.org/math/statistics-probability/describing-relationships-quantitative-data/more-on-regression/v/regression-line-example  # noqa: E501
    """
    xpoints = tuple(point[0] for point in points)
    ypoints = tuple(point[1] for point in points)
    num_points = float(len(points))

    xmean = sum(xpoints) / num_points
    ymean = sum(ypoints) / num_points
    product_mean = sum(x * y for x, y in points) / num_points
    x_square_mean = sum(x * x for x in xpoints) / num_points

    slope = (xmean * ymean - product_mean) / ((xmean * xmean) - x_square_mean)
    intercept = ymean - slope * xmean

    return slope, intercept
