import datetime


def safe_eval(expr):
    """
    Safely evaluate simple default expressions like:
    'lambda: datetime.date.today().isoformat()'
    """
    # Define safe context
    safe_builtins = {
        "None": None,
        "True": True,
        "False": False,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "__import__": __import__
    }
    safe_globals = {
        "datetime": datetime,
        "__builtins__": safe_builtins,  # disable built-in access
    }
    try:
        result = eval(expr, safe_globals)
        return result
    except Exception as e:
        raise ValueError(f"Failed to evaluate default expression: {expr}. Error: {e}")