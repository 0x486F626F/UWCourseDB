import sqlite3
import datetime
from uwaterlooapi   import UWaterlooAPI
uw = UWaterlooAPI(api_key="123afda14d0a233ecb585591a95e0339")

class UWCoursesDB:
	def __init__(self, term):
		self.term = term
		self.sql = sqlite3.connect(str(term) + '.db')
		self.db = self.sql.cursor()
		self.db.execute('''CREATE TABLE IF NOT EXISTS course(
			subject				TEXT,
			catalog_number		TEXT,
			title				TEXT,
			topic				TEXT,
			note				TEXT,
			academic_level		TEXT,
			units				REAL,
			last_updated		TEXT,
			last_sync			TEXT);''')
		self.sql.commit()

	def add_course(self, course):
		self.db.execute('''INSERT INTO course 
			(subject, catalog_number, title, topic, note, academic_level, units, last_updated, last_sync) ''' + "VALUES('" \
				+ str(course[0]['subject']) + "', '" \
				+ str(course[0]['catalog_number']) +"', '" \
				+ str(course[0]['title']) + "', '" \
				+ str(course[0]['topic']) + "', '" \
				+ str(course[0]['note']) + "', '" \
				+ str(course[0]['academic_level']) + "', " \
				+ str(course[0]['units']) + ", '" \
				+ str(course[0]['last_updated']) + "', '" \
				+ str(datetime.datetime.today()) +"');")
		self.db.execute("DROP TABLE IF EXISTS " + course[0]['subject'] + course[0]['catalog_number'] + ";")
		self.db.execute('CREATE TABLE IF NOT EXISTS ' + course[0]['subject'] + course[0]['catalog_number'] + '''(
					class_number		INTEGER,
					section				TEXT,
					associated_class	INTEGER,
					related_component_1	TEXT,
					related_component_2	TEXT,
					campus				TEXT,
					reserve_group		TEXT,
					reserve_total		INTEGER,
					reserve_capacity	INTEGER,
					enrollment_total	INTEGER,
					enrollment_capacity	INTEGER,
					waiting_total		INTEGER,
					waiting_capacity	INTEGER,
					held_with			TEXT);''')
		for section in course:
			self.add_section(section)
		self.sql.commit()

	def add_section(self, section):
		held_with = ''
		for course in section['held_with']:
			held_with += str(course) + '\n'
		course_table = section['subject'] + section['catalog_number']
		self.db.execute('INSERT INTO ' + course_table \
				+ '(class_number, section, associated_class, related_component_1, related_component_2, ' \
				+ "campus, enrollment_total, enrollment_capacity, waiting_total, waiting_capacity, held_with) VALUES(" \
					+ str(section['class_number']) + ", '" \
					+ str(section['section']) + "', " \
					+ str(section['associated_class']) + ", '" \
					+ str(section['related_component_1']) + "', '" \
					+ str(section['related_component_2']) + "', '" \
					+ str(section['campus']) + "', " \
					+ str(section['enrollment_total']) + ", " \
					+ str(section['enrollment_capacity']) + ", " \
					+ str(section['waiting_total']) + ", " \
					+ str(section['waiting_capacity']) + ", '" \
					+ held_with + "');") 

		for reserve in section['reserves']:
			self.db.execute('INSERT INTO ' + course_table \
				+ '(class_number, section, associated_class, related_component_1, related_component_2, ' \
				+ 'campus, reserve_group, reserve_total, reserve_capacity, ' \
				+ "enrollment_total, enrollment_capacity, waiting_total, waiting_capacity, held_with) VALUES(" \
					+ str(section['class_number']) + ", '" \
					+ str(section['section']) + "', " \
					+ str(section['associated_class']) + ", '" \
					+ str(section['related_component_1']) + "', '" \
					+ str(section['related_component_2']) + "', '" \
					+ str(section['campus']) + "', '" \
					+ str(reserve['reserve_group']) + "', " \
					+ str(reserve['enrollment_total']) + ", " \
					+ str(reserve['enrollment_capacity']) + ", " \
					+ str(section['enrollment_total']) + ", " \
					+ str(section['enrollment_capacity']) + ", " \
					+ str(section['waiting_total']) + ", " \
					+ str(section['waiting_capacity']) + ", '" \
					+ held_with + "');") 

		time_table = course_table + section['section'].replace(" ", "") + "_classes" 
		self.db.execute("DROP TABLE IF EXISTS " + time_table + ";")
		self.db.execute('CREATE TABLE IF NOT EXISTS ' + time_table + '' + '''(
			is_tba			TEXT,
			is_cancelled	TEXT,
			is_closed		TEXT,
			start_date		TEXT,
			end_date		TEXT,
			start_time		TEXT,
			end_time		TEXT,
			weekdays		TEXT,
			instructors		TEXT,
			building		TEXT,
			room			TEXT);''')

		for classes in section['classes']:
			instructors = ''
			for instructor in classes['instructors']:
				instructors += str(instructor) + '\n' 
			self.db.execute('INSERT INTO ' + time_table \
					+ '(is_tba, is_cancelled, is_closed, start_date, end_date, start_time, end_time, weekdays, ' \
					+ "instructors, building, room) VALUES('" \
					+ str(classes['date']['is_tba']) + "', '" \
					+ str(classes['date']['is_cancelled']) + "', '" \
					+ str(classes['date']['is_closed']) + "', '" \
					+ str(classes['date']['start_date']) + "', '" \
					+ str(classes['date']['end_date']) + "', '" \
					+ str(classes['date']['start_time']) + "', '" \
					+ str(classes['date']['end_time']) + "', '" \
					+ str(classes['date']['weekdays']) + "', '" \
					+ instructors + "', '" \
					+ str(classes['location']['building']) + "', '" \
					+ str(classes['location']['room']) + "');")

		self.sql.commit()


	def update_course(self, subject, catalog):
		self.db.execute("DELETE FROM course WHERE subject == '" + subject + "' AND catalog_number == '" + catalog + "';")
		course = uw.term_course_schedule(self.term, subject, catalog)
		if (len(course) > 0):
			self.add_course(course)	



db = UWCoursesDB(1159)
db.update_course('CS', '430')
