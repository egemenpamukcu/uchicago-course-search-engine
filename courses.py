'''
Course search engine: search

Egemen Pamukcu
'''

from math import radians, cos, sin, asin, sqrt, ceil
import sqlite3
import os


# Use this filename for the database
DATA_DIR = os.path.dirname(__file__)
DATABASE_FILENAME = os.path.join(DATA_DIR, 'course_information.sqlite3')

conn = sqlite3.connect('course_information.sqlite3')
c = conn.cursor()


def find_courses(args_from_ui):
    '''
    Takes a dictionary containing search criteria and returns courses
    that match the criteria.  The dictionary will contain some of the
    following fields:

      - dept a string
      - day is list of strings
           -> ["'MWF'", "'TR'", etc.]
      - time_start is an integer in the range 0-2359
      - time_end is an integer an integer in the range 0-2359
      - enrollment is a pair of integers
      - walking_time is an integer
      - building_code ia string
      - terms is a list of strings string: ["quantum", "plato"]

    Returns a pair: an ordered list of attribute names and a list the
     containing query results.  Returns ([], []) when the dictionary
     is empty.
    '''
    conn.create_function("time_between", 4, compute_time_between)

    if len(args_from_ui) == 0:
        return ([], [])


    assert_valid_input(args_from_ui)
    select = '''
             SELECT DISTINCT
             courses.dept,
             courses.course_num,
             courses.title
             '''
    add_select1 =\
        '''
         ,
         sections.section_num,
         meeting_patterns.day,
         meeting_patterns.time_start,
         meeting_patterns.time_end,
         sections.enrollment
         '''
    add_select2 =\
        '''
         ,
         sections.building_code,
         time_between(gps.lon, gps.lat, ?, ?) AS walking_time

         '''
    frm =\
        '''
        FROM courses
        '''
    add_joins =\
        '''
        JOIN sections on courses.course_id = sections.course_id
        JOIN meeting_patterns ON sections.meeting_pattern_id = meeting_patterns.meeting_pattern_id
        JOIN gps on gps.building_code = sections.building_code
        '''
    where =\
        '''
        WHERE 1 = 1
        '''
    where_terms =\
        '''
        AND courses.course_id IN
        (SELECT course_id FROM catalog_index
        WHERE word IN ({})
        GROUP BY course_id
        HAVING count(*) = ?)
        '''
    where_dict = {
        'time_start' : ' AND meeting_patterns.time_start >= ?',
        'time_end' : ' AND meeting_patterns.time_end <= ?',
        'day' : ' AND meeting_patterns.day IN ({})'.format(', '.join(['?'] * len(args_from_ui.get('day', [])))),
        'dept' : " AND courses.dept = ?",
        'enrollment' : ' AND sections.enrollment BETWEEN ? AND ?',
        'terms' : where_terms.format(', '.join(['?'] * len(args_from_ui.get('terms', [])))),
        'walking_time' : ' AND walking_time <= ?'
    }
    input_vars = {'enrollment', 'time_start', 'time_end', 'day', 'section_num', 'walking_time', 'building_code'}

    args = []

    if len(set(args_from_ui.keys()) & input_vars) > 0:
        select += add_select1
        frm += add_joins
        if 'building_code' in args_from_ui.keys():
            lonlat = c.execute("select lon, lat from gps where building_code = ?",
                               [args_from_ui['building_code']]).fetchall()[0]
            args += list(lonlat)
            select += add_select2


    for k, v in args_from_ui.items():
        if k != 'building_code':
            where += where_dict[k]
            if type(v) == list:
                args += v
                if k == 'terms':
                    args.append(len(v))
            else:
                args.append(v)

    query = select + frm + where
    result_set = c.execute(query, args)
    return (get_header(result_set), result_set.fetchall())


########### auxiliary functions #################
########### do not change this code #############

def assert_valid_input(args_from_ui):
    '''
    Verify that the input conforms to the standards set in the
    assignment.
    '''

    assert isinstance(args_from_ui, dict)

    acceptable_keys = set(['time_start', 'time_end', 'enrollment', 'dept',
                           'terms', 'day', 'building_code', 'walking_time'])
    assert set(args_from_ui.keys()).issubset(acceptable_keys)

    # get both buiding_code and walking_time or neither
    has_building = ("building_code" in args_from_ui and
                    "walking_time" in args_from_ui)
    does_not_have_building = ("building_code" not in args_from_ui and
                              "walking_time" not in args_from_ui)

    assert has_building or does_not_have_building

    assert isinstance(args_from_ui.get("building_code", ""), str)
    assert isinstance(args_from_ui.get("walking_time", 0), int)

    # day is a list of strings, if it exists
    assert isinstance(args_from_ui.get("day", []), (list, tuple))
    assert all([isinstance(s, str) for s in args_from_ui.get("day", [])])

    assert isinstance(args_from_ui.get("dept", ""), str)

    # terms is a non-empty list of strings, if it exists
    terms = args_from_ui.get("terms", [""])
    assert terms
    assert isinstance(terms, (list, tuple))
    assert all([isinstance(s, str) for s in terms])

    assert isinstance(args_from_ui.get("time_start", 0), int)
    assert args_from_ui.get("time_start", 0) >= 0

    assert isinstance(args_from_ui.get("time_end", 0), int)
    assert args_from_ui.get("time_end", 0) < 2400

    # enrollment is a pair of integers, if it exists
    enrollment_val = args_from_ui.get("enrollment", [0, 0])
    assert isinstance(enrollment_val, (list, tuple))
    assert len(enrollment_val) == 2
    assert all([isinstance(i, int) for i in enrollment_val])
    assert enrollment_val[0] <= enrollment_val[1]


def compute_time_between(lon1, lat1, lon2, lat2):
    '''
    Converts the output of the haversine formula to walking time in minutes
    '''
    meters = haversine(lon1, lat1, lon2, lat2)

    # adjusted downwards to account for manhattan distance
    walk_speed_m_per_sec = 1.1
    mins = meters / (walk_speed_m_per_sec * 60)

    return int(ceil(mins))


def haversine(lon1, lat1, lon2, lat2):
    '''
    Calculate the circle distance between two points
    on the earth (specified in decimal degrees)
    '''
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    # 6367 km is the radius of the Earth
    km = 6367 * c
    m = km * 1000
    return m


def get_header(cursor):
    '''
    Given a cursor object, returns the appropriate header (column names)
    '''
    header = []

    for i in cursor.description:
        s = i[0]
        if "." in s:
            s = s[s.find(".")+1:]
        header.append(s)

    return header
