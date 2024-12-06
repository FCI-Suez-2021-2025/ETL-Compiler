import numpy as np


class DataProcessor:
    def __init__(self):
        pass

    def calculate_wind_speed(self, u_component_of_wind_10m, v_component_of_wind_10m):
        # https://confluence.ecmwf.int/pages/viewpage.action?pageId=133262398
        return np.sqrt(u_component_of_wind_10m**2 + v_component_of_wind_10m**2)

    def calculate_wind_direction(
        self, u_component_of_wind_10m, v_component_of_wind_10m
    ):
        # https://confluence.ecmwf.int/pages/viewpage.action?pageId=133262398
        return np.rad2deg(np.arctan2(u_component_of_wind_10m, v_component_of_wind_10m))

    def calculate_relative_humidity(self, temperature, dewpoint_temp):
        # https://www.wikihow.com/Calculate-Humidity
        temperature = temperature - 273.15
        dewpoint_temp = dewpoint_temp - 273.15

        actual_vapor_pressure = 6.11 * (
            10 ** ((17.625 * dewpoint_temp) / (243.04 + dewpoint_temp))
        )
        saturation_vapor_pressure = 6.11 * (
            10 ** ((17.625 * temperature) / (243.04 + temperature))
        )

        relative_humidity = actual_vapor_pressure / saturation_vapor_pressure

        return relative_humidity

    def calculate_specific_humidity(self, dewpoint_k, pressure):
        # https://carnotcycle.wordpress.com/2020/06/01/how-to-calculate-mixing-ratio-and-specific-humidity/
        pressure_hpa = pressure / 100
        dewpoint_c = dewpoint_k - 273.15

        actual_vapor_pressure = 6.112 * np.exp(
            (17.67 * dewpoint_c) / (dewpoint_c + 243.5)
        )

        specific_humidity = (0.622 * actual_vapor_pressure) / (
            (pressure_hpa) - (0.378 * actual_vapor_pressure)
        )

        return specific_humidity * 1000

    def assign_season(self, date):
        if date.month in [12, 1, 2]:
            return "Winter"
        elif date.month in [3, 4, 5]:
            return "Autumn"
        elif date.month in [6, 7, 8]:
            return "Summer"
        elif date.month in [9, 10, 11]:
            return "Spring"

    def assign_year(self, date):
        return date.year
