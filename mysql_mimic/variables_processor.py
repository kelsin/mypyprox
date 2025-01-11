from datetime import datetime

from sqlglot import expressions as exp

from mysql_mimic.intercept import value_to_expression
from mysql_mimic.variables import Variables


class SessionContext:
    connection_id: int
    external_user: str
    current_user: str
    version: str
    database: str
    variables: Variables
    timestamp: datetime

    def __init__(
        self,
        connection_id: int,
        current_user: str,
        variables: Variables,
        database: str,
        timestamp: datetime,
    ):
        self.connection_id = connection_id
        self.external_user = variables.get("external_user")
        self.variables = variables
        self.current_user = current_user
        self.version = variables.get("version")
        self.database = database
        self.timestamp = timestamp


class VariablesProcessor:

    def __init__(self, session: SessionContext):
        self._session = session
        # Information functions.
        # These will be replaced in the AST with their corresponding values.
        self._functions = {
            "CONNECTION_ID": lambda: session.connection_id,
            "USER": lambda: session.external_user,
            "CURRENT_USER": lambda: session.current_user,
            "VERSION": lambda: session.version,
            "DATABASE": lambda: session.database,
            "NOW": lambda: session.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "CURDATE": lambda: session.timestamp.strftime("%Y-%m-%d"),
            "CURTIME": lambda: session.timestamp.strftime("%H:%M:%S"),
        }
        # Synonyms
        self._functions.update(
            {
                "SYSTEM_USER": self._functions["USER"],
                "SESSION_USER": self._functions["USER"],
                "SCHEMA": self._functions["DATABASE"],
                "CURRENT_TIMESTAMP": self._functions["NOW"],
                "LOCALTIME": self._functions["NOW"],
                "LOCALTIMESTAMP": self._functions["NOW"],
                "CURRENT_DATE": self._functions["CURDATE"],
                "CURRENT_TIME": self._functions["CURTIME"],
            }
        )
        self._constants = {
            "CURRENT_USER",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
        }

    def replace_variables(self, expression: exp.Expression) -> None:
        if isinstance(expression, exp.Set):
            for setitem in expression.expressions:
                if isinstance(setitem.this, exp.Binary):
                    # In the case of statements like: SET @@foo = @@bar
                    # We only want to replace variables on the right
                    setitem.this.set(
                        "expression",
                        setitem.this.expression.transform(self._transform, copy=True),
                    )
        else:
            expression.transform(self._transform, copy=False)

    def _transform(self, node: exp.Expression) -> exp.Expression:
        new_node = None

        if isinstance(node, exp.Func):
            if isinstance(node, exp.Anonymous):
                func_name = node.name.upper()
            else:
                func_name = node.sql_name()
            func = self._functions.get(func_name)
            if func:
                value = func()
                new_node = value_to_expression(value)
        elif isinstance(node, exp.Column) and node.sql() in self._constants:
            value = self._functions[node.sql()]()
            new_node = value_to_expression(value)
        elif isinstance(node, exp.SessionParameter):
            value = self._session.variables.get(node.name)
            new_node = value_to_expression(value)

        if (
            new_node
            and isinstance(node.parent, exp.Select)
            and node.arg_key == "expressions"
        ):
            new_node = exp.alias_(new_node, exp.to_identifier(node.sql()))

        return new_node or node
