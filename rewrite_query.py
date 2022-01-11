import re
import psycopg2
import regex

class PostgresDB:
    """ PostgresDB class represents local Postgres DB connection. """

    def __init__(self):
        self._connection = psycopg2.connect(user = "pgbouncer",
                                            password = "{?;]FAB-^*o|KGqTHWw|(^{>",
                                            host = "10.233.27.141",
                                            port = "5432",
                                            database = "searchiq")
        self._cursor = self._connection.cursor()

    def query(self, query):
        try:
            self._cursor.execute(query)
            result = self._cursor.fetchall()
        except Exception as error:
            print('error executing query "{}", error: {}'.format(query, error))
            return None
        else:
            return result

    def query_params(self, query, params):
        try:
            self._cursor.execute(query, params)
            result = self._cursor.fetchall()
        except Exception as error:
            print('error executing query "{}", error: {}'.format(query, error))
            return None
        else:
            return result

    def __del__(self):
        self._connection.close()

# Global variables
postgres_db = PostgresDB()

block_query_clause = "1<>1"
unlimited_access_query_clause = "1=1"
where_clause_token = "WHERE"
where_clause_validated_token = "RW_WHE"

unlimited_privileged_resource_per_report = {
    "ADVERTISER": "Unlimited_Advertiser_Manager",
    "PROVIDER": "Unlimited_Provider_Manager",
    "FEED": "Unlimited_Provider_Manager",
    "PUBLISHER": "Unlimited_Publisher_Manager"
}

privileged_resource_per_report = {
    "ADVERTISER": "Advertiser_Manager",
    "PROVIDER": "Provider_Manager",
    "FEED": "Provider_Manager",
    "PUBLISHER": "Publisher_Manager"
}

# Functions to get PostgreSQL data by user

def get_user_advertisers(user_id):
    query = """SELECT advertiser_lid
    FROM addotnet.advertiser_user_dim
    WHERE (administrators_lid || '~' || administrators_hid) = '%s'""" % user_id
    return postgres_db.query(query)

def get_executive_advertisers_by_id(user_id):
    query = """SELECT advertiser_lid
    FROM addotnet.advertiser_executives_dim 
    WHERE (executives_lid || '~' || executives_hid) = '%s'""" % user_id
    return postgres_db.query(query)

def get_user_affiliates(user_id):
    query = """SELECT affiliateaccount_lid
    FROM addotnet.affiliateaccount_user_dim
    WHERE (administrators_lid || '~' || administrators_hid) = '%s'""" % user_id
    return postgres_db.query(query)

def get_user_provider_accounts(user_id):
    postgres_db = PostgresDB()
    query = """SELECT provideraccount_lid
    FROM addotnet.provideraccount_user_dim
    WHERE (administrators_lid || '~' || administrators_hid) = '%s'""" % user_id
    return postgres_db.query(query)

def get_user_feed_advertisers(user_id):
    query = """SELECT feedadvertiser_id
    FROM addotnet.feedadvertiser_user_dim
    WHERE (administrators_lid || '~' || administrators_hid) = '%s'""" % user_id
    return postgres_db.query(query)

def get_user_privileged_resources(user_id):
    query = """SELECT pr.name
    FROM addotnet.user_privilegedresource_dim upr LEFT JOIN addotnet.privilegedresource_dim pr ON upr.privilegedResources_lid=pr.lid AND upr.privilegedresources_hid=pr.hid
    WHERE (user_lid || '~' || user_hid) = '%s'""" % user_id
    return postgres_db.query(query)

def get_executive_advertisers(username):
    query = """SELECT adve.advertiser_lid
    FROM addotnet.user_dim ud LEFT JOIN addotnet.advertiser_executives_dim adve ON ud.lid=adve.executives_lid AND ud.hid=adve.executives_hid
    WHERE ud.username = '%s'""" % username
    return postgres_db.query(query)

def get_manager_advertisers(username):
    query = """SELECT advu.advertiser_lid
    FROM addotnet.user_dim ud LEFT JOIN addotnet.advertiser_user_dim advu ON ud.lid=advu.administrators_lid AND ud.hid=advu.administrators_hid
    WHERE ud.username = '%s'""" % username
    return postgres_db.query(query)


# Functions to define user permissions

def build_clause(ids_dataset, id_label):
    if len(ids_dataset) > 0:
        clause = "%s IN (" % id_label
        for id_element in ids_dataset:
            clause = clause + ("'%s'," % id_element[0])

        clause = clause[:-1] + ")"
        return clause
    else:
        return None

def build_advertiser_clause(user_id):
    advertisers_dataset = get_user_advertisers(user_id)
    executive_advertisers_dataset = get_executive_advertisers_by_id(user_id)
    advertisers_dataset.extend(executive_advertisers_dataset)
    return build_clause(advertisers_dataset,"advertiser_lid")

def build_affiliate_clause(user_id):
    affiliates_dataset = get_user_affiliates(user_id)
    return build_clause(affiliates_dataset,"affiliate_account_lid")

def build_provider_account_clause(user_id):
    provider_accounts_dataset = get_user_provider_accounts(user_id)
    return build_clause(provider_accounts_dataset,"provider_account_lid")

def build_feed_advertiser_clause(user_id):
    feed_advertisers_dataset = get_user_feed_advertisers(user_id)
    return build_clause(feed_advertisers_dataset,"feed_advertiser_id")

def has_unlimited_report_access(report_type, privileged_resources):
    print("Looking for unlimited privileged report access")
    if privileged_resources and len(privileged_resources) > 0:
        if report_type in unlimited_privileged_resource_per_report:
            unlimited_privileged_resource = unlimited_privileged_resource_per_report.get(report_type)
            if unlimited_privileged_resource in privileged_resources:
                print("User has unlimited access[%s] for report type[%s]" % (unlimited_privileged_resource, report_type))
                return True
        else:
            print("Report type[%s] is not valid" % report_type)
            return False

    print("User doesn't have unlimited privileged access for report type[%s]" % report_type)
    return False

def has_report_access(report_type, privileged_resources):
    print("Looking for privileged report access")
    if privileged_resources and len(privileged_resources) > 0:
        if report_type in privileged_resource_per_report:
            privileged_resource = privileged_resource_per_report.get(report_type)
            if report_type == "ADVERTISER":
                if privileged_resource in privileged_resources or "Advertiser_Sales" in privileged_resources:
                    print("User has access[%s] for report type[%s]" % (privileged_resource, report_type))
                    return True
            else:
                if privileged_resource in privileged_resources:
                    print("User has access[%s] for report type[%s]" % (privileged_resource, report_type))
                    return True
        else:
            print("Report type[%s] is not valid" % report_type)
            return False

    print("User doesn't have privileged access for report type[%s]" % report_type)
    return False

def generate_permission_clause(user_id, report_type):
    print("Generating permission clause")
    permission_clause = None
    privileged_resources_dataset = get_user_privileged_resources(user_id)
    privileged_resources = []

    for row in privileged_resources_dataset:
        privileged_resources.extend([row[0]])

    if has_unlimited_report_access(report_type, privileged_resources):
        permission_clause = unlimited_access_query_clause
    elif has_report_access(report_type, privileged_resources):
        if report_type == "ADVERTISER":
            permission_clause = build_advertiser_clause(user_id)
        elif report_type == "PROVIDER":
            permission_clause = build_provider_account_clause(user_id)
        elif report_type == "FEED":
            permission_clause = build_feed_advertiser_clause(user_id)
        elif report_type == "PUBLISHER":
            permission_clause = build_affiliate_clause(user_id)
        else:
            print("Unable to generate permission clause. Report type[%s] is not supported" % report_type)
    else:
        print("Unable to generate permission clause. User doesn't have privileged resource access for report type[%s]" % report_type)

    if permission_clause:
        return permission_clause
    else:
        return block_query_clause

# Functions to rewrite filters

def build_executive_clause(username):
    advertisers_dataset = get_executive_advertisers(username)
    return build_clause(advertisers_dataset,"advertiser_lid")

def build_manager_clause(username):
    advertisers_dataset = get_manager_advertisers(username)
    return build_clause(advertisers_dataset,"advertiser_lid")

def generate_executive_user_clause(executive_username):
    print("Generating executive user clause")
    executive_clause = build_executive_clause(executive_username)
    if executive_clause:
        return executive_clause
    else:
        return block_query_clause

def generate_manager_user_clause(manager_username):
    print("Generating manager user clause")
    manager_clause = build_manager_clause(manager_username)
    if manager_clause:
        return manager_clause
    else:
        return block_query_clause

# Util functions

def elim_outer(query, num_parenthesis):
    last_query = query
    for i in range(num_parenthesis):
        output = re.search('\((.*)\)', last_query, re.MULTILINE).group(1)
        if output is None:
            return last_query
        else:
            last_query = output
    return last_query

def rewrite_where_clause(where_clause):
    # Executive user filter
    if "ad_event.executive_user_id" in where_clause:
        print("Looking for executive_user_id in where_clause")
        executive_user_pattern = 'ad_event\.executive_user_id\s*=\s*\'(.*?\|(.*?))\''

        if re.search(executive_user_pattern, where_clause):
            current_executive_clause = re.search(executive_user_pattern, where_clause).group(0)
            executive_username = re.search(executive_user_pattern, where_clause).group(2)

            new_executive_clause = generate_executive_user_clause(executive_username.strip())
            if new_executive_clause:
                where_clause = where_clause.replace(current_executive_clause, new_executive_clause)
            else:
                print("Unable to replace executive_user_id clause for where_clause[%s]" % where_clause)
        else:
            print("Unable to find executive_user_id pattern in clause[%s]" % where_clause)

    # Manager user filter
    if "manager_user_id" in where_clause:
        print("Looking for manager_user_id in where_clause")
        manager_user_pattern = 'manager_user_id\s*=\s*\'(.*?\|(.*?))\''

        if re.search(manager_user_pattern, where_clause):
            current_manager_clause = re.search(manager_user_pattern, where_clause).group(0)
            manager_username = re.search(manager_user_pattern, where_clause).group(2)

            new_manager_clause = generate_manager_user_clause(manager_username.strip())
            if new_manager_clause:
                where_clause = where_clause.replace(current_manager_clause, new_manager_clause)
            else:
                print("Unable to replace manager_user_id clause for where_clause[%s]" % where_clause)
        else:
            print("Unable to find manager_user_id pattern in clause[%s]" % where_clause)

    # Permissions
    if "user_id" in where_clause and "report_type" in where_clause:
        print("Looking for user_id and report_type parameters in where_clause")
        user_report_pattern = '.*(user_id\s*=\s*\'(-?\d+~-?\d+)\'\s*AND\s*report_type\s*=\s*\'(\w+)\').*'
        report_user_pattern = '.*(report_type\s*=\s*\'(\w+)\'\s*AND\s*user_id\s*=\s*\'(-?\d+~-?\d+)\').*'
        current_permission_clause = None

        if re.match(user_report_pattern, where_clause):
            current_permission_clause = re.match(user_report_pattern, where_clause).group(1)
        elif re.match(report_user_pattern, where_clause):
            current_permission_clause = re.match(report_user_pattern, where_clause).group(1)
        else:
            raise Exception("Unable to find user_id and report_type pattern in where_clause[%s]. You should use \"user_id = 'xx' AND report_type = 'xx'\" or \"report_type = 'xx' AND user_id = 'xx'\"" % where_clause)

        user_id = None
        report_type = None

        if "user_id" in current_permission_clause and "report_type" in current_permission_clause:
            user_id_pattern = '.*user_id\s*=\s*\'(-?\d+~-?\d+)\''
            if re.match(user_id_pattern, current_permission_clause):
                user_id = re.match(user_id_pattern, current_permission_clause).group(1)
            else:
                raise Exception("Unable to find user_id pattern in clause[%s]" % current_permission_clause)

            report_type_pattern = '.*report_type\s*=\s*\'(\w+)\''
            if re.match(report_type_pattern, current_permission_clause):
                report_type = re.match(report_type_pattern, current_permission_clause).group(1)
            else:
                raise Exception("Unable to find report_type pattern in clause[%s]" % current_permission_clause)

            if user_id and report_type:
                new_permission_clause = generate_permission_clause(user_id, report_type)
                if new_permission_clause:
                    where_clause = where_clause.replace(current_permission_clause, new_permission_clause)
                else:
                    raise Exception("Unable to replace user permissions for where_clause[%s]" % where_clause)
            else:
                raise Exception("Not possible to extract user_id or report_type values from where_clause[%s]" % where_clause)

    # Timestamp
    if "TIMESTAMP" in where_clause:
        timestamp_pattern = "\(TIMESTAMP\s+(\'\d+-\d+-\d+\')\)"
        if re.search(timestamp_pattern, where_clause):
            for timestamp_clause in re.finditer(timestamp_pattern, where_clause):
                sub_timestamp = timestamp_clause.group()
                where_clause = where_clause.replace(sub_timestamp, timestamp_clause.group(1))

    # Interval
    while "INTERVAL" in where_clause:
        for clause_iterator in regex.finditer('\(([^()]|(?R))*\)', where_clause):
            clause = clause_iterator.group()
            if "INTERVAL" in clause:
                interval_clauses = re.split("AND|OR", clause, re.MULTILINE)
                connection = None
                cursor = None

                try:
                    connection = psycopg2.connect(user = "postgres",
                                                  password = "adnet2019",
                                                  host = "127.0.0.1",
                                                  port = "5432",
                                                  database = "postgres")
                    cursor = connection.cursor()
                    # Print PostgreSQL Connection properties
                    print ( connection.get_dsn_parameters(),"\n")

                    interval_clause_0 = re.search('.*[>=<]+(.*)', interval_clauses[0], re.MULTILINE).group(1)
                    subselect0 = regex.search('\((?>[^()]|(?R))*\)', interval_clause_0, re.MULTILINE).group(0)
                    cursor.execute(subselect0)
                    result0 = cursor.fetchone()
                    where_clause = where_clause.replace(subselect0, "'"+result0[0].strftime("%Y-%m-%d %H:%M:%S")+"'")

                    if len(interval_clauses) == 2:
                        if re.search('.*[>=<]+(.*)', interval_clauses[1], re.MULTILINE) is not None:
                            interval_clause_1 = re.search('.*[>=<]+(.*)', interval_clauses[1], re.MULTILINE).group(1)
                            subselect1 = regex.search('\((?>[^()]|(?R))*\)', interval_clause_1, re.MULTILINE).group(0)
                            cursor.execute(subselect1)
                            result1 = cursor.fetchone()
                            where_clause = where_clause.replace(subselect1, "'"+result1[0].strftime("%Y-%m-%d %H:%M:%S")+"'")

                except (Exception, psycopg2.Error) as error :
                    print ("Error while connecting to PostgreSQL", error)
                    raise Exception("INTERVAL Clause is having parsing error, or query error, or connection error")
                finally:
                    #closing database connection.
                    if connection:
                        cursor.close()
                        connection.close()
                        print("PostgreSQL connection is closed")

    # Add ad_event."dt" for query performance
    if 'ad_event."event_date"' in where_clause:
        lower_event_date_pattern = ".*ad_event[.]\"event_date\".*>=\s*'(\d{4}-\d{2}-\d{2}).*?'.*"
        if re.match(lower_event_date_pattern, where_clause) and len(re.match(lower_event_date_pattern, where_clause).groups()) == 1:
            lower_event_date = re.match(lower_event_date_pattern, where_clause).group(1)
            where_clause = ' ad_event."dt" >= ' + "'" + lower_event_date + "' AND " + where_clause

    if 'DATE_TRUNC' in where_clause  and 'day' in where_clause and 'SELECT' not in where_clause:
        #pure date trunc replacement
        where_clause = re.sub(r'DATE_TRUNC\(\'day\', DATE ([0-9\-\']+)\)', r'\1', where_clause)

    return where_clause


def rewrite_subquery(query):
    subquery_pattern = '\((([^()]|(?R))*)\)'

    for subquery in regex.finditer(subquery_pattern, query):
        current_subquery = subquery.group(1)
        new_subquery = rewrite_subquery(current_subquery)
        query = query.replace(current_subquery, new_subquery, 1)

    if "WHERE" in query:
        where_clause_pattern = ".*(WHERE(.*)(GROUP BY|ORDER BY|HAVING|LIMIT)?)"

        for where_clause in re.finditer(where_clause_pattern, query):
            current_where_clause = where_clause.group(2)
            new_where_clause = rewrite_where_clause(current_where_clause)
            query = query.replace(current_where_clause, new_where_clause, 1)
            query = query.replace(where_clause_token, where_clause_validated_token,1)

    return query


def rewrite_query(username, query):

    # handle looker transaction
    if "Looker Query Context" in query:
        query_rewritten = query.replace("\n"," ")
        query_rewritten_pattern = "(-- Looker Query Context.*?\'{.*?}\')(.*)"

        if re.match(query_rewritten_pattern, query_rewritten):
            looker_part = re.match(query_rewritten_pattern, query_rewritten).group(1)
            query_part = re.match(query_rewritten_pattern, query_rewritten).group(2)

            if looker_part and query_part:
                query_part = rewrite_subquery(query_part)
                query_part = query_part.replace(where_clause_validated_token, where_clause_token)
                query = looker_part + "\n" + query_part
    elif "SELECT" in query and "WHERE" in query:
        query = query.replace("\n"," ")
        query = rewrite_subquery(query)
        query = query.replace(where_clause_validated_token, where_clause_token)

    return query

if __name__ == "__main__":
    # some tests

    '''
    #User don't have access to Advertiser report - block query
    print rewrite_query("master", """SELECT advertiser_name, advertiser_lid, advertiser_hid
    FROM addotnet.request_click
    WHERE user_id = '-4715694947553513527~-3468402077022468804' AND report_type = 'ADVERTISER'""")
    '''

    '''
    #User have unlimited access to Advertiser report
    print rewrite_query("master", """SELECT advertiser_name, advertiser_lid, advertiser_hid
    FROM addotnet.request_click
    WHERE report_type = 'ADVERTISER' AND user_id = '-5425678528862634270~7610230789594105028'""")
    '''

    '''
    #User have unlimited access to Advertiser report, filter by advertiser_name
    print rewrite_query("master", """SELECT advertiser_name, advertiser_lid, advertiser_hid
    FROM addotnet.request_click
    WHERE user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER' AND advertiser_name = 'Macy'""")
    '''

    '''
    print rewrite_query("master", """SELECT advertiser_name, advertiser_lid, advertiser_hid
    FROM addotnet.request_click
    WHERE col1=2 OR user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER' AND col2 = 'value'""")
    '''

    '''
    print rewrite_query("master", """SELECT DATE(search_request."dt" ) AS "search_request.dt_date"
    FROM public.search_request  AS search_request
    WHERE (((search_request."dt" ) >= ((SELECT DATE_TRUNC('day', (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP)) + (-2 || ' month')::INTERVAL)))) AND (search_request."dt" ) < ((SELECT DATE_TRUNC('day', ((DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP)) + (-2 || ' month')::INTERVAL) + (3 || ' month')::INTERVAL)))))) AND user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER'
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 500""")
    '''

    '''
    print rewrite_query("master", """SELECT
    DATE(search_request."dt" ) AS "search_request.dt_date"
    FROM public.search_request  AS search_request

    WHERE
    (((search_request."dt" ) >= ((SELECT DATE_TRUNC('day', (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP)) + (-2 || ' month')::INTERVAL)))) AND (search_request."dt" ) < ((SELECT DATE_TRUNC('day', ((DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP)) + (-2 || ' month')::INTERVAL) + (3 || ' month')::INTERVAL))))))
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 500""")
    '''

    '''
    print rewrite_query("master", """SELECT
    DATE(search_request."dt" ) AS "search_request.dt_date"
    FROM public.search_request  AS search_request
    
    WHERE
    (((search_request."dt" ) >= (DATE_TRUNC('day', DATE '2019-06-02')) AND (search_request."dt" ) < (DATE_TRUNC('day', DATE '2019-06-10'))))
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 500""")
    '''

    '''
    print rewrite_query("master", "SELECT prodname, SUM(total) FROM sales JOIN product USING (productid) GROUP BY prodname ORDER BY prodname;")
    '''

    '''
    # Query with filters
    print rewrite_query("master", """SELECT
    DATE(ad_event."event_date" ) AS "ad_event.event_date",
    ad_event."advertiser_lid"  AS "ad_event.advertiser_lid",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."raw_clicks" ), 0) AS "ad_event.raw_clicks",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    COALESCE(SUM(ad_event."event_fires_count" ), 0) AS "ad_event.event_fires_count",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth"
    FROM addotnet.ad_event  AS ad_event

    WHERE (ad_event."advertiser_lid"  = -5560081301784095000) AND ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL))))) AND user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER'
    GROUP BY 1,2
    ORDER BY 1 DESC
    LIMIT 500""") 
    '''

    '''
    # Query with filters
    print rewrite_query("master", """SELECT 
    request_click."advertiser_hid"  AS "request_click.advertiser_hid",
    request_click."advertiser_lid"  AS "request_click.advertiser_lid",
    DATE(request_click."click_date" ) AS "request_click.click_date"
    FROM addotnet.request_click  AS request_click

    WHERE ((((request_click."click_date" ) >= ((SELECT DATE_TRUNC('day', CURRENT_TIMESTAMP))) AND (request_click."click_date" ) < ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (1 || ' day')::INTERVAL)))))) AND (user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER')
    GROUP BY 1,2,3
    ORDER BY 3 DESC
    LIMIT 500""")
    '''

    """
    print rewrite_query('master', '''
    SELECT
    DATE(ad_event."event_date" ) AS "ad_event.event_date",
    ad_event."advertiser_lid"  AS "ad_event.advertiser_lid",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."raw_clicks" ), 0) AS "ad_event.raw_clicks",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    COALESCE(SUM(ad_event."event_fires_count" ), 0) AS "ad_event.event_fires_count",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth"
    FROM addotnet.ad_event  AS ad_event

    WHERE (ad_event."advertiser_lid"  = -5560081301784095000) AND ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL))))))
    GROUP BY 1,2
    ORDER BY 1 DESC
    LIMIT 500
    ''')
    """

    """
    print rewrite_query('master','''
    SELECT
    DATE(ad_event."event_date" ) AS "ad_event.event_date",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue",
    COALESCE(SUM(ad_event."requests" ), 0) AS "ad_event.requests"
    FROM addotnet.ad_event_view AS ad_event

    WHERE (((ad_event."advertiser_name") = 'Macys Department')) AND ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL) + (2 || ' day')::INTERVAL))))))
    GROUP BY 1,2,3,4
    ORDER BY 8 DESC
    LIMIT 500''')
    """

    '''
    # Query with executive and manager filters
    print rewrite_query("master", """SELECT 
    request_click."advertiser_hid"  AS "request_click.advertiser_hid",
    request_click."advertiser_lid"  AS "request_click.advertiser_lid",
    DATE(request_click."click_date" ) AS "request_click.click_date"
    FROM addotnet.request_click  AS request_click

    WHERE ((((request_click."click_date" ) >= ((SELECT DATE_TRUNC('day', CURRENT_TIMESTAMP))) AND (request_click."click_date" ) < ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (1 || ' day')::INTERVAL)))))) AND (user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER') AND ad_event.executive_user_id = 'Aaron Baff | aaron' OR (ad_event.manager_user_id = 'Antony Nguyen | antonyn')
    GROUP BY 1,2,3
    ORDER BY 3 DESC
    LIMIT 500""")
    '''

    '''
    # Query with nested sub queries
    print rewrite_query("master", """SELECT * FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
    SELECT *, MIN(z___rank) OVER (PARTITION BY "ad_event.event_date","ad_event.advertiser_status") as z___min_rank FROM (
    SELECT *, RANK() OVER (ORDER BY CASE WHEN z__pivot_col_rank=1 THEN (CASE WHEN "ad_event.revenue" IS NOT NULL THEN 0 ELSE 1 END) ELSE 2 END, CASE WHEN z__pivot_col_rank=1 THEN "ad_event.revenue" ELSE NULL END DESC, "ad_event.revenue" DESC, z__pivot_col_rank, "ad_event.event_date", "ad_event.advertiser_status") AS z___rank FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "ad_event.advertiser_name" IS NULL THEN 1 ELSE 0 END, "ad_event.advertiser_name", CASE WHEN "ad_event.advertiser_lid" IS NULL THEN 1 ELSE 0 END, "ad_event.advertiser_lid", CASE WHEN "ad_event.advertiser_hid" IS NULL THEN 1 ELSE 0 END, "ad_event.advertiser_hid") AS z__pivot_col_rank FROM (
    SELECT
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    DATE(ad_event."event_date" ) AS "ad_event.event_date",
    ad_event."advertiser_status" AS "ad_event.advertiser_status",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue",
    COALESCE(SUM(ad_event."requests" ), 0) AS "ad_event.requests"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL)))))) AND (1 <> 1)
    GROUP BY 1,2,3,4,5) ww
    ) bb WHERE z__pivot_col_rank <= 16384
    ) aa
    ) xx
    ) zz
    WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank""")
    '''

    '''
    print rewrite_query("master", """SELECT * FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
    SELECT *, MIN(z___rank) OVER (PARTITION BY "ad_event.event_date","ad_event.advertiser_status") as z___min_rank FROM (
    SELECT *, RANK() OVER (ORDER BY CASE WHEN z__pivot_col_rank=1 THEN (CASE WHEN "ad_event.revenue" IS NOT NULL THEN 0 ELSE 1 END) ELSE 2 END, CASE WHEN z__pivot_col_rank=1 THEN "ad_event.revenue" ELSE NULL END DESC, "ad_event.revenue" DESC, z__pivot_col_rank, "ad_event.event_date", "ad_event.advertiser_status") AS z___rank FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "ad_event.advertiser_name" IS NULL THEN 1 ELSE 0 END, "ad_event.advertiser_name", CASE WHEN "ad_event.advertiser_lid" IS NULL THEN 1 ELSE 0 END, "ad_event.advertiser_lid", CASE WHEN "ad_event.advertiser_hid" IS NULL THEN 1 ELSE 0 END, "ad_event.advertiser_hid") AS z__pivot_col_rank FROM (
    SELECT
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    DATE(ad_event."event_date" ) AS "ad_event.event_date",
    ad_event."advertiser_status" AS "ad_event.advertiser_status",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue",
    COALESCE(SUM(ad_event."requests" ), 0) AS "ad_event.requests"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL)))))) AND (1 <> 1)
    GROUP BY 1,2,3,4,5) ww
    ) bb WHERE z__pivot_col_rank <= 16384
    ) aa
    ) xx
    ) zz
    WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) AND user_id = '-5425678528862634270~7610230789594105028' AND report_type = 'ADVERTISER' ORDER BY z___pivot_row_rank""")
    '''

    '''
    # Query with new line characters in where clause
    print rewrite_query("master", """SELECT
	ad_event_daily_feed_adjustment."advertiser_name"  AS "ad_event_daily_feed_adjustment.advertiser_name",
	ad_event_daily_feed_adjustment."io_revenue_cap"  AS "ad_event_daily_feed_adjustment.io_revenue_cap",
	ad_event_daily_feed_adjustment."io_total_day"  AS "ad_event_daily_feed_adjustment.io_total_day",
	ad_event_daily_feed_adjustment."io_elapsed_day"  AS "ad_event_daily_feed_adjustment.io_elapsed_day",
	COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0) AS "ad_event_daily_feed_adjustment.revenue",
	CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_spent_percent",
	CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_day_elapsed_percent",
	CASE WHEN (CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END) > 0 THEN ((CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END)/(CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END)) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_pacing_percent",
	CASE WHEN (ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_elapsed_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_avg_daily_spend",
	CASE WHEN (ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN (((ad_event_daily_feed_adjustment."io_revenue_cap")-(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0)))/((ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day"))) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_target_daily_spend"
    FROM addotnet.ad_event_daily_adjustment  AS ad_event_daily_feed_adjustment

    WHERE 1=1 AND
    (user_id = '-7868085493690489810~-6035181592612876413' AND report_type = 'ADVERTISER')
    and (DATE(ad_event_daily_feed_adjustment."io_start_date" )) is not null and (DATE(ad_event_daily_feed_adjustment."io_start_date" )) > '2019-09-11'
    GROUP BY 1,2,3,4
    ORDER BY 5 DESC
    LIMIT 500""")
    '''

    '''
    # Query with Looker Query Context
    print rewrite_query("master", """-- Looker Query Context '{"user_id":3,"history_id":8881,"instance_slug":"22ffaa880ef383c45fc9e22e67caea3e"}' SELECT  ad_event."advertiser_name" AS "ad_event.advertiser_name", ad_event."advertiser_lid" AS "ad_event.advertiser_lid", ad_event."advertiser_hid" AS "ad_event.advertiser_hid", ad_event."advertiser_status" AS "ad_event.advertiser_status", COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns", COALESCE(SUM(ad_event."raw_clicks" ), 0) AS "ad_event.raw_clicks", COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks", CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc", CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa", COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth", COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue" FROM addotnet.ad_event_view AS ad_event  WHERE ((((ad_event."event_date" ) >= ("2019-09-24 00:00:00") AND (ad_event."event_date" ) < ("2019-09-25 00:00:00")))) AND (1=1 AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1) GROUP BY 1,2,3,4 ORDER BY 11 DESC#012LIMIT 500""")
    '''

    '''
    # Query with no user_id
    print rewrite_query("master", """SELECT
    ad_event."adgroup_name" AS "ad_event.adgroup_name",
    ad_event."adgroup_lid" AS "ad_event.adgroup_lid",
    ad_event."adgroup_hid" AS "ad_event.adgroup_hid",
    ad_event."adgroup_status" AS "ad_event.adgroup_status",
    CASE WHEN length(ad_event.daily_revenue_caps)>1
    THEN substring(ad_event.daily_revenue_caps, position('=' in ad_event.daily_revenue_caps)+1,(position(',' in ad_event.daily_revenue_caps)-position('=' in ad_event.daily_revenue_caps)-1))
    ELSE '' END AS "ad_event.daily_revenue_caps",
    ad_event."cpa_goal" AS "ad_event.cpa_goal",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."raw_clicks" ), 0) AS "ad_event.raw_clicks",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc",
    CASE WHEN (COALESCE(SUM(ad_event."ad_returns" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."paid_clicks" ), 0))/(COALESCE(SUM(ad_event."ad_returns" ), 0)) ELSE 0 END AS "ad_event.ctr",
    CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT DATE_TRUNC('day', CURRENT_TIMESTAMP))) AND (ad_event."event_date" ) < ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (1 || ' day')::INTERVAL)))))) AND (1 = 1
    AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1)
    GROUP BY 1,2,3,4,5,6
    ORDER BY 14 DESC
    LIMIT 500""")
    '''

    '''
    # Query with multiple WHERE
    print rewrite_query("master", """SELECT * FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
    SELECT *, MIN(z___rank) OVER (PARTITION BY "ad_event.advertiser_name","ad_event.advertiser_lid","ad_event.advertiser_hid") as z___min_rank FROM (
    SELECT *, RANK() OVER (ORDER BY CASE WHEN z__pivot_col_rank=1 THEN (CASE WHEN "ad_event.revenue" IS NOT NULL THEN 0 ELSE 1 END) ELSE 2 END, CASE WHEN z__pivot_col_rank=1 THEN "ad_event.revenue" ELSE NULL END DESC, "ad_event.revenue" DESC, z__pivot_col_rank, "ad_event.advertiser_name", "ad_event.advertiser_lid", "ad_event.advertiser_hid") AS z___rank FROM (
    SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "ad_event.event_date" IS NULL THEN 1 ELSE 0 END, "ad_event.event_date") AS z__pivot_col_rank FROM (
    SELECT
    DATE(ad_event."event_date" ) AS "ad_event.event_date",
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-6 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL)))))) AND (user_id = '-7868085493690489810~-6035181592612876413' AND report_type = 'ADVERTISER'
    AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1)
    GROUP BY 1,2,3,4) ww
    ) bb WHERE z__pivot_col_rank <= 16384
    ) aa
    ) xx
    ) zz
    WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank""")
    '''

    """
    # User with Advertiser_Manager permission
    print rewrite_query("master", '''SELECT
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    ad_event."advertiser_status" AS "ad_event.advertiser_status",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc",
    CASE WHEN (COALESCE(SUM(ad_event."ad_returns" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."paid_clicks" ), 0))/(COALESCE(SUM(ad_event."ad_returns" ), 0)) ELSE 0 END AS "ad_event.ctr",
    CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT DATE_TRUNC('day', CURRENT_TIMESTAMP))) AND (ad_event."event_date" ) < ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (1 || ' day')::INTERVAL)))))) AND (user_id = '-9114147125155635901~-5275064534496490628' AND report_type = 'ADVERTISER'
    AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1)
    GROUP BY 1,2,3,4
    ORDER BY 11 DESC
    LIMIT 500''')
    """

    '''
    # Query with multiple manager user ids (we don't have support for this since manager choice is only 1 in AAA)
    print rewrite_query("master", """SELECT
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    ad_event."advertiser_status" AS "ad_event.advertiser_status",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc",
    CASE WHEN (COALESCE(SUM(ad_event."ad_returns" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."paid_clicks" ), 0))/(COALESCE(SUM(ad_event."ad_returns" ), 0)) ELSE 0 END AS "ad_event.ctr",
    CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT DATE_TRUNC('day', CURRENT_TIMESTAMP))) AND (ad_event."event_date" ) < ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP) + (1 || ' day')::INTERVAL)))))) AND ((ad_event.manager_user_id IN ('Antony Nguyen | adnettestlive', 'Antony Nguyen | antonyn'))) AND (user_id = '-7868085493690489810~-6035181592612876413' AND report_type = 'ADVERTISER'
    AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1)
    GROUP BY 1,2,3,4
    ORDER BY 11 DESC
    LIMIT 500""")
    '''

    """
    # IOCap report with date is not null at the beginning of clause 
    print rewrite_query("master", '''SELECT
	ad_event_daily_feed_adjustment."advertiser_name"  AS "ad_event_daily_feed_adjustment.advertiser_name",
	ad_event_daily_feed_adjustment."io_revenue_cap"  AS "ad_event_daily_feed_adjustment.io_revenue_cap",
	ad_event_daily_feed_adjustment."io_total_day"  AS "ad_event_daily_feed_adjustment.io_total_day",
	ad_event_daily_feed_adjustment."io_elapsed_day"  AS "ad_event_daily_feed_adjustment.io_elapsed_day",
	DATE(ad_event_daily_feed_adjustment."io_start_date" ) AS "ad_event_daily_feed_adjustment.io_start_date",
	DATE(ad_event_daily_feed_adjustment."io_end_date" ) AS "ad_event_daily_feed_adjustment.io_end_date",
	COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0) AS "ad_event_daily_feed_adjustment.revenue",
	CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_spent_percent",
	CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_day_elapsed_percent",
	((CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END)/(CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END)) AS "ad_event_daily_feed_adjustment.io_pacing_percent",
	CASE WHEN (ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_elapsed_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_avg_daily_spend",
	CASE WHEN (ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN (((ad_event_daily_feed_adjustment."io_revenue_cap")-(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0)))/((ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day"))) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_target_daily_spend"
    FROM addotnet.ad_event_daily_adjustment  AS ad_event_daily_feed_adjustment
    
    WHERE ((DATE(ad_event_daily_feed_adjustment."io_start_date" )) is not null and ((( (DATE(ad_event_daily_feed_adjustment."io_start_date" )) ) >= ((SELECT TIMESTAMP '2019-09-01')) AND ( (DATE(ad_event_daily_feed_adjustment."io_start_date" )) ) < ((SELECT (TIMESTAMP '2019-09-01' + (1 || ' day')::INTERVAL)))))) AND (1 = 1)
    GROUP BY 1,2,3,4,5,6
    ORDER BY 7 DESC
    LIMIT 500''')
    """

    """
    # IOCap report with user_id and date is not null at the beginning of clause
    print rewrite_query("master", '''SELECT
    ad_event_daily_feed_adjustment."advertiser_name"  AS "ad_event_daily_feed_adjustment.advertiser_name",
    ad_event_daily_feed_adjustment."io_revenue_cap"  AS "ad_event_daily_feed_adjustment.io_revenue_cap",
    ad_event_daily_feed_adjustment."io_total_day"  AS "ad_event_daily_feed_adjustment.io_total_day",
    ad_event_daily_feed_adjustment."io_elapsed_day"  AS "ad_event_daily_feed_adjustment.io_elapsed_day",
    DATE(ad_event_daily_feed_adjustment."io_start_date" ) AS "ad_event_daily_feed_adjustment.io_start_date",
    DATE(ad_event_daily_feed_adjustment."io_end_date" ) AS "ad_event_daily_feed_adjustment.io_end_date",
    COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0) AS "ad_event_daily_feed_adjustment.revenue",
    CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_spent_percent",
    CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_day_elapsed_percent",
    ((CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END)/(CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END)) AS "ad_event_daily_feed_adjustment.io_pacing_percent",
    CASE WHEN (ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_elapsed_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_avg_daily_spend",
    CASE WHEN (ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN (((ad_event_daily_feed_adjustment."io_revenue_cap")-(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0)))/((ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day"))) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_target_daily_spend"
    FROM addotnet.ad_event_daily_adjustment  AS ad_event_daily_feed_adjustment
    
    WHERE ((DATE(ad_event_daily_feed_adjustment."io_start_date" )) is not null and ((( (DATE(ad_event_daily_feed_adjustment."io_start_date" )) ) >= ((SELECT TIMESTAMP '2019-09-01')) AND ( (DATE(ad_event_daily_feed_adjustment."io_start_date" )) ) < ((SELECT (TIMESTAMP '2019-09-01' + (1 || ' day')::INTERVAL)))))) AND (user_id = '-7868085493690489810~-6035181592612876413' AND report_type = 'ADVERTISER')
    GROUP BY 1,2,3,4,5,6
    ORDER BY 7 DESC
    LIMIT 500''')
    """

    """
    # Two dates clauses
    print rewrite_query("master", '''SELECT
    ad_event_daily_feed_adjustment."advertiser_name" AS "ad_event_daily_feed_adjustment.advertiser_name",
    ad_event_daily_feed_adjustment."io_revenue_cap" AS "ad_event_daily_feed_adjustment.io_revenue_cap",
    ad_event_daily_feed_adjustment."io_total_day" AS "ad_event_daily_feed_adjustment.io_total_day",
    ad_event_daily_feed_adjustment."io_elapsed_day" AS "ad_event_daily_feed_adjustment.io_elapsed_day",
    DATE(ad_event_daily_feed_adjustment."io_start_date" ) AS "ad_event_daily_feed_adjustment.io_start_date",
    DATE(ad_event_daily_feed_adjustment."io_end_date" ) AS "ad_event_daily_feed_adjustment.io_end_date",
    COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0) AS "ad_event_daily_feed_adjustment.revenue",
    CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_spent_percent",
    CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_day_elapsed_percent",
    ((CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END)/(CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END)) AS "ad_event_daily_feed_adjustment.io_pacing_percent",
    CASE WHEN (ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_elapsed_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_avg_daily_spend",
    CASE WHEN (ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN (((ad_event_daily_feed_adjustment."io_revenue_cap")-(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0)))/((ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day"))) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_target_daily_spend",
    COALESCE(SUM(ad_event_daily_feed_adjustment."today_revenue" ), 0) AS "ad_event_daily_feed_adjustment.today_revenue"
    FROM addotnet.ad_event_daily_adjustment AS ad_event_daily_feed_adjustment
    
    WHERE ((ad_event_daily_feed_adjustment."io_end_date" >= (SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (7 || ' day')::INTERVAL)))) AND ((DATE(ad_event_daily_feed_adjustment."io_start_date" )) is not null and ((( (DATE(ad_event_daily_feed_adjustment."io_start_date" )) ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (-59 || ' day')::INTERVAL))) AND ( (DATE(ad_event_daily_feed_adjustment."io_start_date" )) ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (-59 || ' day')::INTERVAL) + (60 || ' day')::INTERVAL)))))) AND (1 = 1)
    GROUP BY 1,2,3,4,5,6
    ORDER BY 6 DESC
    LIMIT 500''')
    """

    """
    # After 60 days query
    print rewrite_query("master", '''SELECT
    ad_event_daily_feed_adjustment."advertiser_name"  AS "ad_event_daily_feed_adjustment.advertiser_name",
    ad_event_daily_feed_adjustment."io_revenue_cap"  AS "ad_event_daily_feed_adjustment.io_revenue_cap",
    ad_event_daily_feed_adjustment."io_total_day"  AS "ad_event_daily_feed_adjustment.io_total_day",
    ad_event_daily_feed_adjustment."io_elapsed_day"  AS "ad_event_daily_feed_adjustment.io_elapsed_day",
    DATE(ad_event_daily_feed_adjustment."io_start_date" ) AS "ad_event_daily_feed_adjustment.io_start_date",
    DATE(ad_event_daily_feed_adjustment."io_end_date" ) AS "ad_event_daily_feed_adjustment.io_end_date",
    COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0) AS "ad_event_daily_feed_adjustment.revenue",
    CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_spent_percent",
    CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_day_elapsed_percent",
    ((CASE WHEN (ad_event_daily_feed_adjustment."io_revenue_cap") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_revenue_cap")) ELSE 0 END)/(CASE WHEN (ad_event_daily_feed_adjustment."io_total_day") > 0 THEN ((ad_event_daily_feed_adjustment."io_elapsed_day")::decimal/(ad_event_daily_feed_adjustment."io_total_day")) ELSE 0 END)) AS "ad_event_daily_feed_adjustment.io_pacing_percent",
    CASE WHEN (ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN ((COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0))/(ad_event_daily_feed_adjustment."io_elapsed_day")) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_avg_daily_spend",
    CASE WHEN (ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day") > 0 THEN (((ad_event_daily_feed_adjustment."io_revenue_cap")-(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue" ), 0)))/((ad_event_daily_feed_adjustment."io_total_day")-(ad_event_daily_feed_adjustment."io_elapsed_day"))) ELSE 0 END AS "ad_event_daily_feed_adjustment.io_target_daily_spend",
    COALESCE(SUM(ad_event_daily_feed_adjustment."today_revenue" ), 0) AS "ad_event_daily_feed_adjustment.today_revenue"
    FROM addotnet.ad_event_daily_adjustment  AS ad_event_daily_feed_adjustment
    
    WHERE ((ad_event_daily_feed_adjustment."io_end_date"  >= (SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (60 || ' day')::INTERVAL)))) AND (1 = 1)
    GROUP BY 1,2,3,4,5,6
    ORDER BY 7 DESC
    LIMIT 500''')
    """

    """
    # Add dt for ad_event_view with TIMESTAMP
    print rewrite_query("master", '''SELECT
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    ad_event."advertiser_status" AS "ad_event.advertiser_status",
    COALESCE(SUM(ad_event."ad_returns" ), 0) AS "ad_event.ad_returns",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc",
    CASE WHEN (COALESCE(SUM(ad_event."ad_returns" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."paid_clicks" ), 0))/(COALESCE(SUM(ad_event."ad_returns" ), 0)) ELSE 0 END AS "ad_event.ctr",
    CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= (TIMESTAMP '2019-11-05') AND (ad_event."event_date" ) < (TIMESTAMP '2019-11-06')))) AND (user_id = '-8979498924215544668~-4793645142632019104' AND report_type = 'ADVERTISER'
    AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1)
    GROUP BY 1,2,3,4
    ORDER BY 11 DESC
    LIMIT 500''')
    """

    """
    # Add dt for ad_event_view with YYYY:MM:DD hh:mm:ss format
    print rewrite_query("master", '''SELECT
    ad_event."advertiser_name" AS "ad_event.advertiser_name",
    ad_event."advertiser_lid" AS "ad_event.advertiser_lid",
    ad_event."advertiser_hid" AS "ad_event.advertiser_hid",
    ad_event."affiliate_account_name" AS "ad_event.publisher_name",
    ad_event."affiliate_account_lid" AS "ad_event.affiliate_account_lid",
    ad_event."affiliate_account_hid" AS "ad_event.affiliate_account_hid",
    ad_event."event_hour" AS "ad_event.event_hour",
    COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
    CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc",
    CASE WHEN (COALESCE(SUM(ad_event."ad_returns" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."paid_clicks" ), 0))/(COALESCE(SUM(ad_event."ad_returns" ), 0)) ELSE 0 END AS "ad_event.ctr",
    CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa",
    COALESCE(SUM(ad_event."actions_worth" ), 0) AS "ad_event.actions_worth",
    COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles'))) AND (ad_event."event_date" ) < ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (1 || ' day')::INTERVAL)))))) AND (user_id = '-8979498924215544668~-4793645142632019104' AND report_type = 'ADVERTISER'
    AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1)
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 13 DESC
    LIMIT 500''')
    """

    """print(rewrite_query("master", '''SELECT
	ad_event."advertiser_name"  AS "ad_event.advertiser_name",
	ad_event."advertiser_lid"  AS "ad_event.advertiser_lid",
	ad_event."advertiser_hid"  AS "ad_event.advertiser_hid",
	ad_event."advertiser_status"  AS "ad_event.advertiser_status",
	COALESCE(SUM(ad_event."paid_clicks" ), 0) AS "ad_event.paid_clicks",
	CASE WHEN (COALESCE(SUM(ad_event."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."paid_clicks" ), 0)) ELSE 0 END AS "ad_event.avg_cpc",
	CASE WHEN (COALESCE(SUM(ad_event."actions_worth" ), 0)) > 0 THEN (COALESCE(SUM(ad_event."revenue" ), 0))/(COALESCE(SUM(ad_event."actions_worth" ), 0)) ELSE 0 END AS "ad_event.cpa",
	COALESCE(SUM(ad_event."revenue" ), 0) AS "ad_event.revenue"
    FROM addotnet.ad_event_view  AS ad_event
    
    WHERE ((((ad_event."event_date" ) >= ((SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (-6 || ' day')::INTERVAL))) AND (ad_event."event_date" ) < ((SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') + (-6 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL)))))) AND (1 = 1
        AND (ad_event."advertiser_name") is not null and (ad_event."advertiser_lid") <> -1) AND user_id = '-6119083734667510450~4673457212217248999' AND report_type = 'ADVERTISER'
    GROUP BY 1,2,3,4
    ORDER BY 8 DESC
    LIMIT 500'''))"""

    """print(rewrite_query("master",'''SELECT
	ad_event_daily_feed_adjustment."affiliate_account_name"  AS "ad_event_daily_feed_adjustment.affiliate_account_name",
	ad_event_daily_feed_adjustment."affiliate_account_lid"  AS "ad_event_daily_feed_adjustment.affiliate_account_lid",
	ad_event_daily_feed_adjustment."affiliate_account_hid"  AS "ad_event_daily_feed_adjustment.affiliate_account_hid",
	COALESCE(SUM(ad_event_daily_feed_adjustment."requests" ), 0) AS "ad_event_daily_feed_adjustment.requests",
	COALESCE(SUM(ad_event_daily_feed_adjustment."raw_clicks" ), 0) AS "ad_event_daily_feed_adjustment.raw_clicks",
	COALESCE(SUM(ad_event_daily_feed_adjustment."paid_clicks" ), 0) AS "ad_event_daily_feed_adjustment.paid_clicks",
	CASE WHEN (COALESCE(SUM(ad_event_daily_feed_adjustment."requests" ), 0)) > 0 THEN (COALESCE(SUM(ad_event_daily_feed_adjustment."paid_clicks" ), 0))/(COALESCE(SUM(ad_event_daily_feed_adjustment."requests" ), 0)) ELSE 0 END AS "ad_event_daily_feed_adjustment.ctr",
	CASE WHEN (COALESCE(SUM(ad_event_daily_feed_adjustment."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0))/(COALESCE(SUM(ad_event_daily_feed_adjustment."paid_clicks" ), 0)) ELSE 0 END AS "ad_event_daily_feed_adjustment.avg_cpc",
	CASE WHEN (COALESCE(SUM(ad_event_daily_feed_adjustment."paid_clicks" ), 0)) > 0 THEN (COALESCE(SUM(ad_event_daily_feed_adjustment."actions_worth" ), 0))/(COALESCE(SUM(ad_event_daily_feed_adjustment."paid_clicks" ), 0)) ELSE 0 END AS "ad_event_daily_feed_adjustment.conversion",
	COALESCE(SUM(ad_event_daily_feed_adjustment."actions_worth" ), 0) AS "ad_event_daily_feed_adjustment.actions_worth",
	COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0) AS "ad_event_daily_feed_adjustment.revenue",
	COALESCE(SUM(ad_event_daily_feed_adjustment."pub_payout" ), 0) AS "ad_event_daily_feed_adjustment.pub_payout",
	COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" - ad_event_daily_feed_adjustment."pub_payout"), 0) AS "ad_event_daily_feed_adjustment.revenue_net",
	CASE WHEN (COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0)) > 0 THEN (COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" - ad_event_daily_feed_adjustment.pub_payout - ad_event_daily_feed_adjustment.pub_payout_diff), 0))/(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0)) ELSE 0 END AS "ad_event_daily_feed_adjustment.margin",
	COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0) AS "ad_event_daily_feed_adjustment.actual_revenue",
	COALESCE(SUM(ad_event_daily_feed_adjustment."pub_payout" +ad_event_daily_feed_adjustment.pub_payout_diff), 0) AS "ad_event_daily_feed_adjustment.actual_pub_payout",
	COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" - ad_event_daily_feed_adjustment.pub_payout - ad_event_daily_feed_adjustment.pub_payout_diff), 0) AS "ad_event_daily_feed_adjustment.actual_revenue_net",
	CASE WHEN (COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0)) > 0 THEN (COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" - ad_event_daily_feed_adjustment.pub_payout - ad_event_daily_feed_adjustment.pub_payout_diff), 0))/(COALESCE(SUM(ad_event_daily_feed_adjustment."revenue"+ ad_event_daily_feed_adjustment."revenue_diff" ), 0)) ELSE 0 END AS "ad_event_daily_feed_adjustment.actual_margin",
	CASE WHEN (COALESCE(SUM(ad_event_daily_feed_adjustment."requests" ), 0)) > 0 THEN (COALESCE(SUM(ad_event_daily_feed_adjustment."pub_payout" +ad_event_daily_feed_adjustment.pub_payout_diff), 0))*1000/(COALESCE(SUM(ad_event_daily_feed_adjustment."requests" ), 0)) ELSE 0 END AS "ad_event_daily_feed_adjustment.ecpm"
    FROM addotnet.ad_event_daily_adjustment_budget  AS ad_event_daily_feed_adjustment
    WHERE ((((ad_event_daily_feed_adjustment."event_date" ) >= (DATE_TRUNC('day', DATE '2020-06-03')) AND (ad_event_daily_feed_adjustment."event_date" ) < (DATE_TRUNC('day', DATE '2020-06-04'))))) AND (user_id = '-6119083734667510450~4673457212217248999' AND report_type = 'PUBLISHER'
        AND (ad_event_daily_feed_adjustment."affiliate_account_lid") is not null)
    GROUP BY 1,2,3
    ORDER BY 11 DESC
    LIMIT 500'''))"""
