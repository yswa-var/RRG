from sqlalchemy import URL, make_url, MetaData, create_engine, text
from utils import is_true
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='db/db.log', encoding='utf-8',level=logging.DEBUG)


class BaseDbConn:
    rdbms_type = ""
    driver_pkg = ""
    default_db = ""

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.user = kwargs.get('user')
        self.pwd = kwargs.get('pwd')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.db_name = kwargs.get('db_name')
        self.read_only = kwargs.get('read_only', True)

        db_conn_url = kwargs.get("db_conn_url", None)
        if db_conn_url:
            self.engine_str = str(db_conn_url)
        else:
            self.engine_str = f'{self.rdbms_scheme}://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db_name}'
        # Construct URL
        self.engine_url: URL = make_url(self.engine_str)
        # Update values
        self.host = self.engine_url.host
        self.db_name = self.engine_url.database
        self.read_only = is_true(self.engine_url.query.get('read_only', self.read_only))
        # Remove custom parameters
        query_dict = dict(self.engine_url.query)
        for _custom in ['read_only']:
            query_dict.pop(_custom, None)
        self.engine_url = self.engine_url.set(drivername=self.rdbms_scheme, query=query_dict)
        self.temp_engine_url: URL = self.engine_url.set(database=self.default_db, query=query_dict)

        self._conn_pool = None
        self.pool_size = kwargs.get('pool_size', 2)
        self.pool_overflow = kwargs.get('pool_overflow', 10)

    @property
    def rdbms_scheme(self):
        return f"{self.rdbms_type}+{self.driver_pkg}"

    @property
    def pool(self):
        if self._conn_pool is None:
            self.init_pool()
        return self._conn_pool

    def init_pool(self, force=False):
        if self._conn_pool is None or force:
            logger.info(f'{"*" * 10} Pool Initialized for: {self.rdbms_type} {self.db_name} {"*" * 10}')
            self._conn_pool = sql.create_engine(self.engine_url, pool_size=self.pool_size,
                                                max_overflow=self.pool_overflow,
                                                pool_recycle=67, pool_timeout=30, echo=None)


class PgSQLSchema(BaseSchema):
    req_sps = []
    req_funcs = []
    req_triggers = []
    req_m_views = []

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


    def define_relations(self):
        raise NotImplementedError('Required method not defined')


    def load_default_data(self):
        pass

    def check_db_status(self):
        if not self.db_conn.read_only:
            with create_engine(self.db_conn.temp_engine_url, isolation_level='AUTOCOMMIT').connect() as conn:
                res = conn.execute(text(f"select * from pg_database where datname='{self.db_conn.db_name}';"))
                # noinspection PyTypeChecker
                rows = res.rowcount > 0
                if not rows:
                    # conn.execute('commit')
                    res_db = conn.execute(text(f'CREATE DATABASE {self.db_conn.db_name};'))
                    print(f"Created {self.db_conn.db_name} DB in {self.db_conn.host}. Response: {res_db.rowcount}")

    def check_tables(self):
        super().check_tables()

    def check_stored_procedures(self):
        sp_dir = self.scripts_dir_loc()
        # Check required SPs
        with self.meta_engine.connect() as conn:
            query = """
                select n.nspname, p.proname
                FROM pg_catalog.pg_namespace n
                JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid
                WHERE p.prokind = 'p'
                AND n.nspname = 'public';
            """
            res = conn.execute(text(query))
            rows = res.fetchall()
            available_sps = [_r[1] for _r in rows]
            for _sp in self.req_sps:
                if _sp not in available_sps:
                    expected_path = os.path.join(sp_dir, f"{_sp}.sql")
                    if os.path.isfile(expected_path):
                        with open(expected_path, 'r') as in_file:
                            sp_source = ''.join(in_file.readlines())
                            sp_resp = conn.execute(sql.sql.expression.text(sp_source))
                            conn.execute(sql.sql.expression.text('commit;'))
                            print(
                                f'Created SP {_sp} from {expected_path} in {self.db_conn.db_name}@{self.db_conn.host}: {sp_resp.rowcount}')
                    else:
                        raise RuntimeWarning(
                            f'Required stored procedure not available: {_sp}.\nExpected Location: {expected_path}')

    def check_db_functions(self):
        func_dir = self.scripts_dir_loc()
        # Check required Functions
        with self.meta_engine.connect() as conn:
            query = """
                select n.nspname, p.proname
                FROM pg_catalog.pg_namespace n
                JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid
                WHERE p.prokind = 'f'
                AND n.nspname = 'public';
            """
            res = conn.execute(text(query))
            rows = res.fetchall()
            available_funcs = [_r[1] for _r in rows]
            for _func in self.req_funcs:
                if _func not in available_funcs:
                    expected_path = os.path.join(func_dir, f"{_func}.sql")
                    if os.path.isfile(expected_path):
                        with open(expected_path, 'r') as in_file:
                            func_source = ''.join(in_file.readlines())
                            func_resp = conn.execute(sql.sql.expression.text(func_source))
                            print(
                                f'Created Function {_func} from {expected_path} in {self.db_conn.db_name}@{self.db_conn.host}: {func_resp.rowcount}')
                    else:
                        raise RuntimeWarning(
                            f'Required function not available: {_func}.\nExpected Location: {expected_path}')

    def check_db_triggers(self):
        trigger_dir = self.scripts_dir_loc()
        # Check required SPs
        with self.meta_engine.connect() as conn:
            query = """
                SELECT 'public' as nspname, tgname proname
                FROM  pg_trigger
                WHERE tgisinternal is false;
            """
            res = conn.execute(text(query))
            rows = res.fetchall()
            available_triggers = [_r[1] for _r in rows]
            for _trigger in self.req_triggers:
                if _trigger not in available_triggers:
                    expected_path = os.path.join(trigger_dir, f"{_trigger}.sql")
                    if os.path.isfile(expected_path):
                        with open(expected_path, 'r') as in_file:
                            trigger_source = ''.join(in_file.readlines())
                            trigger_resp = conn.execute(sql.sql.expression.text(trigger_source))
                            print(
                                f'Created Trigger {_trigger} from {expected_path} in {self.db_conn.db_name}@{self.db_conn.host}: {trigger_resp.rowcount}')
                    else:
                        raise RuntimeWarning(
                            f'Required stored procedure not available: {_trigger}.\nExpected Location: {expected_path}')

    def check_db_material_views(self):
        views_dir = self.scripts_dir_loc()
        # Check required SPs
        with self.meta_engine.connect() as conn:
            query = """
                select matviewname from pg_catalog.pg_matviews ;
            """
            res = conn.execute(text(query))
            rows = res.fetchall()
            available_m_views = [_r[0] for _r in rows]
            for _m_view in self.req_m_views:
                if _m_view not in available_m_views:
                    expected_path = os.path.join(views_dir, f"{_m_view}.sql")
                    if os.path.isfile(expected_path):
                        with open(expected_path, 'r') as in_file:
                            m_view_source = ''.join(in_file.readlines())
                            m_view_resp = conn.execute(sql.sql.expression.text(m_view_source))
                            conn.execute(sql.sql.expression.text('commit;'))
                            print(
                                f'Created MaterialView {_m_view} from {expected_path} in {self.db_conn.db_name}@{self.db_conn.host}: {m_view_resp.rowcount}')
                    else:
                        raise RuntimeWarning(
                            f'Required stored procedure not available: {_m_view}.\nExpected Location: {expected_path}')

    def build_schemas(self):
        super().build_schemas()

    def insert_data(self, table, dict_data, ignore=False):
        """
        NOTE: To be called from load_default_data function only.
        This is used to insert the data in dict format into the table
        :param table: SQLAlchemy Table Object
        :param dict_data: Data to be inserted
        :param ignore: Use Insert Ignore while insertion
        """
        logger.info(f'Data Insertion started for {table.name}')
        ins = table.insert()
        if ignore:
            from sqlalchemy.dialects.postgresql import insert
            ins = insert(table).on_conflict_do_nothing()

        with self.meta_engine.connect() as conn:
            conn.execute(ins, dict_data)
            conn.commit()
            conn.close()

        logger.debug(f"Data Inserted in {table.name}")
