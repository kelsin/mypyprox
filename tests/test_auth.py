from contextlib import closing

import pytest
from mysql.connector import DatabaseError
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.plugins.mysql_clear_password import MySQLClearPasswordAuthPlugin

from mysql_mimic import User
from mysql_mimic.auth import (
    get_mysql_native_password_auth_string,
    MysqlNativePasswordAuthPlugin,
    GullibleAuthPlugin,
    AbstractMysqlClearPasswordAuthPlugin,
)
from tests.conftest import query, to_thread

# mysql.connector throws an error if you try to use mysql_clear_password without SSL.
# That's silly, since SSL termination doesn't have to be handled by MySQL.
# But it's extra silly in tests.
MySQLClearPasswordAuthPlugin.requires_ssl = False
MySQLConnectionAbstract.is_secure = True  # pylint: disable=protected-access

SIMPLE_AUTH_USER = "levon_helm"

PASSWORD_AUTH_USER = "rick_danko"
PASSWORD_AUTH_PASSWORD = "nazareth"
PASSWORD_AUTH_PLUGIN = MysqlNativePasswordAuthPlugin.client_plugin_name


class TestPlugin(AbstractMysqlClearPasswordAuthPlugin):
    name = "test_plugin"

    async def check(self, username, password):
        return username == password


TEST_PLUGIN_AUTH_USER = "garth_hudson"
TEST_PLUGIN_AUTH_PASSWORD = TEST_PLUGIN_AUTH_USER
TEST_PLUGIN_AUTH_PLUGIN = TestPlugin.client_plugin_name

UNKNOWN_PLUGIN_USER = "richard_manuel"
NO_PLUGIN_USER = "miss_moses"


USERS = {
    SIMPLE_AUTH_USER: User(name=SIMPLE_AUTH_USER, auth_plugin=GullibleAuthPlugin.name),
    PASSWORD_AUTH_USER: User(
        name=PASSWORD_AUTH_USER,
        auth_string=get_mysql_native_password_auth_string(PASSWORD_AUTH_PASSWORD),
        auth_plugin=MysqlNativePasswordAuthPlugin.name,
    ),
    TEST_PLUGIN_AUTH_USER: User(
        name=TEST_PLUGIN_AUTH_USER,
        auth_string=TEST_PLUGIN_AUTH_PASSWORD,
        auth_plugin=TestPlugin.name,
    ),
    UNKNOWN_PLUGIN_USER: User(name=UNKNOWN_PLUGIN_USER, auth_plugin="unknown"),
    NO_PLUGIN_USER: User(
        name=NO_PLUGIN_USER,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_plugins,username,password,auth_plugin",
    [
        (
            [MysqlNativePasswordAuthPlugin()],
            PASSWORD_AUTH_USER,
            PASSWORD_AUTH_PASSWORD,
            PASSWORD_AUTH_PLUGIN,
        ),
        (
            [TestPlugin()],
            TEST_PLUGIN_AUTH_USER,
            TEST_PLUGIN_AUTH_PASSWORD,
            TEST_PLUGIN_AUTH_PLUGIN,
        ),
        ([GullibleAuthPlugin()], SIMPLE_AUTH_USER, None, None),
        ([TestPlugin(), GullibleAuthPlugin()], SIMPLE_AUTH_USER, None, None),
        (None, SIMPLE_AUTH_USER, None, None),
    ],
)
async def test_auth(
    server, session, connect, auth_plugins, username, password, auth_plugin
):
    session.use_sqlite = True
    session.users = USERS
    kwargs = {"user": username, "password": password, "auth_plugin": auth_plugin}
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    with closing(await connect(**kwargs)) as conn:
        assert await query(conn=conn, sql="SELECT USER() AS a") == [{"a": username}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_plugins,user1,user2",
    [
        (
            [MysqlNativePasswordAuthPlugin(), TestPlugin()],
            (PASSWORD_AUTH_USER, PASSWORD_AUTH_PASSWORD, PASSWORD_AUTH_PLUGIN),
            (TEST_PLUGIN_AUTH_USER, TEST_PLUGIN_AUTH_PASSWORD, TEST_PLUGIN_AUTH_PLUGIN),
        ),
        (
            [GullibleAuthPlugin()],
            (TEST_PLUGIN_AUTH_USER, TEST_PLUGIN_AUTH_PASSWORD, TEST_PLUGIN_AUTH_PLUGIN),
            (PASSWORD_AUTH_USER, PASSWORD_AUTH_PASSWORD, PASSWORD_AUTH_PLUGIN),
        ),
    ],
)
async def test_change_user(server, session, connect, auth_plugins, user1, user2):
    session.use_sqlite = True
    session.users = USERS
    kwargs1 = {"user": user1[0], "password": user1[1], "auth_plugin": user1[2]}
    kwargs1 = {k: v for k, v in kwargs1.items() if v is not None}

    with closing(await connect(**kwargs1)) as conn:
        # mysql.connector doesn't have great support for COM_CHANGE_USER
        # Here, we have to manually override the auth plugin to use
        conn._auth_plugin = user2[2]  # pylint: disable=protected-access

        await to_thread(conn.cmd_change_user, username=user2[0], password=user2[1])
        assert await query(conn=conn, sql="SELECT USER() AS a") == [{"a": user2[0]}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_plugins,username,password,auth_plugin,msg",
    [
        ([GullibleAuthPlugin()], None, None, None, "User  does not exist"),
        (None, "unknown", None, None, "User unknown does not exist"),
        (
            [TestPlugin()],
            PASSWORD_AUTH_USER,
            PASSWORD_AUTH_PASSWORD,
            PASSWORD_AUTH_PLUGIN,
            "Access denied",
        ),
    ],
)
async def test_access_denied(
    server, session, connect, auth_plugins, username, password, auth_plugin, msg
):
    session.use_sqlite = True
    session.users = USERS
    kwargs = {"user": username, "password": password, "auth_plugin": auth_plugin}
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    with pytest.raises(DatabaseError) as ctx:
        await connect(**kwargs)

    assert msg in str(ctx.value)
