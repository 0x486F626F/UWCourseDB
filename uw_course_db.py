import sqlite3
import datetime

class UWCourseDB:
	def __init__(self, term, uwapi, timedelta = 3600, path = 'db/'): #{{{
		self.term = term
		self.uwapi = uwapi
		self.path = path
		self.sql = sqlite3.connect(path + str(term) + '.db')
		self.db = self.sql.cursor()
		self.min_timedelta = datetime.timedelta(seconds = timedelta)
		self.create_table_if_not_exists("course", [
			"subject		TEXT",
			"catalog_number	TEXT",
			"title			TEXT",
			"topic			TEXT",
			"note			TEXT",
			"academic_level	TEXT",
			"units			REAL",
			"last_updated	TEXT",
			"last_sync		TEXT"])
		#}}}

	def create_table_if_not_exists(self, table_name, headers): #{{{
		command = 'CREATE TABLE IF NOT EXISTS ' + table_name + '('
		for header in headers:
			command += header + ', '
		command = command[:-2] + ');'
		self.db.execute(command)
		self.sql.commit()
		#}}}

	def insert_data(self, table_name, header_value_pairs): #{{{
		command = 'INSERT INTO ' + table_name + '('
		for pair in header_value_pairs:
			command += pair[0] + ', '
		command = command[:-2] + ') VALUES('
		for pair in header_value_pairs:
			value = pair[1]
			if type(value) is str:
				command += "'" + value + "', "
			else:
				command += str(value) + ", "
		command = command[:-2] + ');'
		self.db.execute(command)
		self.sql.commit()
		#}}}
	
	def update_data(self, table_name, header_value_pairs, condition): #{{{
		command = 'UPDATE ' + table_name + ' SET '
		for pair in header_value_pairs:
			header = pair[0]
			value = pair[1]
			if type(value) is str:
				command += header + " = '" + value + "', "
			else:
				command += header + " = " + str(value) + ", "
		command = command[:-2] + ' WHERE ' + condition + ";"
		self.db.execute(command)
		self.sql.commit()
		#}}}

	def update_course(self, subject, catalog): #{{{
		self.db.execute("SELECT last_sync FROM course WHERE subject = '" + subject + \
				"' AND catalog_number = '" + catalog + "';")
		result = self.db.fetchone()

		if (result is not None):
			last_sync = datetime.datetime.strptime(str(result[0]), "%Y-%m-%d %H:%M:%S.%f")
			if (datetime.datetime.today() - last_sync < self.min_timedelta):
				print "No need to update " + subject + catalog
				return

		course = self.uwapi.term_course_schedule(self.term, subject, catalog)
		if (len(course) > 0):
			header_value_pairs =  [
					['subject',			str(course[0]['subject'])],
					['catalog_number',	str(course[0]['catalog_number'])],
					['title',			str(course[0]['title'])],
					['topic',			str(course[0]['topic'])],
					['note',			str(course[0]['note'])],
					['academic_level',	str(course[0]['academic_level'])],
					['units',			course[0]['units']],
					['last_updated',	str(course[0]['last_updated'])],
					['last_sync',		str(datetime.datetime.today())]]
			if (result is None):
				self.insert_data("course", header_value_pairs)
			else:
				self.update_data("course", header_value_pairs,
					"subject = '" + subject + "' AND catalog_number = '" + catalog + "'")
				
			self.db.execute('DROP TABLE IF EXISTS ' + subject + catalog + ";")
			self.create_table_if_not_exists(subject + catalog, [
				'class_number			INTEGER',
				'section				TEXT',
				'associated_class		INTEGER',
				'related_component_1	TEXT',
				'related_component_2	TEXT',
				'campus					TEXT',
				'reserve_group			TEXT',
				'reserve_total			INTEGER',
				'reserve_capacity		INTEGER',
				'enrollment_total		INTEGER',
				'enrollment_capacity	INTEGER',
				'waiting_total			INTEGER',
				'waiting_capacity		INTEGER',
				'held_with				TEXT'])
			for section in course:
				self.update_section(section)
			self.sql.commit()
		else:
			print subject + " " + catalog + " is not offerrd this term"
		#}}}

	def update_section(self, section): #{{{
		course_table = section['subject'] + section['catalog_number']


		held_with = ''
		for course in section['held_with']:
			held_with += str(course) + '\n'
		section_header_value_pairs = [
				['class_number',		str(section['class_number'])],
				['section',				str(section['section'])],
				['associated_class',	str(section['associated_class'])],
				['related_component_1', str(section['related_component_1'])],
				['related_component_2', str(section['related_component_2'])],
				['campus',				str(section['campus'])],
				['enrollment_total',	section['enrollment_total']],
				['enrollment_capacity',	section['enrollment_capacity']],
				['waiting_total',		section['waiting_total']],
				['waiting_capacity',	section['waiting_capacity']],
				['held_with',			held_with]]

		self.insert_data(course_table, section_header_value_pairs)

		for reserve in section['reserves']:
			reserve_header_value_pairs = section_header_value_pairs[:]
			reserve_header_value_pairs.append(['reserve_group',		str(reserve['reserve_group'])])
			reserve_header_value_pairs.append(['reserve_total',		str(reserve['enrollment_total'])])
			reserve_header_value_pairs.append(['reserve_capacity',	str(reserve['enrollment_capacity'])])
			self.insert_data(course_table, reserve_header_value_pairs)

		time_schedule = course_table + section['section'].replace(" ", "") + "_schedule"
		self.db.execute('DROP TABLE IF EXISTS ' + time_schedule + ";")
		self.create_table_if_not_exists(time_schedule, [
			'is_tba			TEXT',
			'is_cancelled	TEXT',
			'is_closed		TEXT',
			'start_date		TEXT',
			'end_date		TEXT',
			'start_time		TEXT',
			'end_time		TEXT',
			'weekdays		TEXT',
			'instructors	TEXT',
			'building		TEXT',
			'room			TEXT']);

		for classes in section['classes']:
			instructors = ''
			for instructor in classes['instructors']:
				instructors += str(instructor) + '\n' 
			schedule_header_value_pairs = [
				['is_tba',		str(classes['date']['is_tba'])],
				['is_cancelled',str(classes['date']['is_cancelled'])],
				['is_closed',	str(classes['date']['is_closed'])],
				['start_date',	str(classes['date']['start_date'])],
				['end_date',	str(classes['date']['end_date'])],
				['start_time',	str(classes['date']['start_time'])],
				['end_time',	str(classes['date']['end_time'])],
				['weekdays',	str(classes['date']['weekdays'])],
				['instructors',	instructors],
				['building',	str(classes['location']['building'])],
				['room',		str(classes['location']['room'])]]
			self.insert_data(time_schedule, schedule_header_value_pairs)

		self.sql.commit()
		#}}}

