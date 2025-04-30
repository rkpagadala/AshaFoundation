from fractions import Fraction


def localeValue(value):
    return "{:,}".format(int(value))

def get_ratio(value1, value2, decimal_places=2):
    try:
        # Ensure both inputs are numbers
        value1 = float(value1)
        value2 = float(value2)
        
        # Handle division by zero
        if value2 == 0:
            return "Division by zero is not allowed"
        
        # Calculate the ratio
        ratio = value1 / value2
        
        # Round the ratio to the specified number of decimal places
        rounded_ratio = round(ratio, decimal_places)
        
        # Convert the ratio to a fraction and simplify it
        fraction = Fraction(rounded_ratio).limit_denominator()
        
        # Format the ratio as a string
        return f"{fraction.numerator}:{fraction.denominator}"
    except ValueError:
        return "Both inputs must be numbers"

