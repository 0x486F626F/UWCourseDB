import sqlite3
import datetime
import os
import uwaterlooapi

class UWCourseDB:

	
	def __init__(self, term, uwapi, timedelta = 3600, path = 'db/'): #{{{
		"""The constructor of the UWCourseDB

		Args:
			term (int): A 4-digit int representing a term (e.g. 1159)
			uwapi (uwaterlooapi): The uwaterloo api created by UWaterlooAPI
					module, get one API key at https://api.uwaterloo.ca/
			timedelta (int): The minimum time gap between database updates
			path (string): The directory you want your local database to be
					located at
		"""
		self.term = term
		self.uwapi = uwapi
		self.path = path
		if not os.path.exists(path):
			os.makedirs(path)
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
		"""Create a table with specified headers in the database

		Args:
			table_name (string): The name of the table
			headers (string): The headers specified to be in the table
		"""

		command = 'CREATE TABLE IF NOT EXISTS ' + table_name + '('
		for header in headers:
			command += header + ', '
		command = command[:-2] + ');'
		self.db.execute(command)
		self.sql.commit()
		#}}}

	def insert_data(self, table_name, header_value_pairs): #{{{
		"""Insert the data into the table
		"""
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
		'''Updates the course specified if it is the first update ever, or it 
		has been more than min_timedelta since last update.

		'''
		self.db.execute("SELECT last_sync FROM course WHERE subject = '" + \
                        subject + "' AND catalog_number = '" + catalog + "';")
		result = self.db.fetchone()

		if (result is not None):
			last_sync = datetime.datetime.strptime(
                str(result[0]), "%Y-%m-%d %H:%M:%S.%f")
			if (datetime.datetime.today() - last_sync < self.min_timedelta):
				# print "No need to update " + subject + catalog
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
					"subject = '" + subject + \
                                 "' AND catalog_number = '" + catalog + "'")
				
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
			print subject + " " + catalog + " is not offerrd for " + str(self.term)
		#}}}

	def update_section(self, section): #{{{
		course_table = section['subject'] + section['catalog_number']

		held_with = ''
		for course in section['held_with']:
			held_with += str(course) + '\n'
		associated_class = str(section['associated_class'])
		related_component_1 = str(section['related_component_1'])
		related_component_2 = str(section['related_component_2'])
		if (associated_class == '99'):
			associated_class = 'None'
		if (related_component_1 == '99'):
			related_component_1 = 'None'
		if (related_component_2 == '99'):
			related_component_2 = 'None'
		section_header_value_pairs = [
				['class_number',		str(section['class_number'])],
				['section',				str(section['section'])],
				['associated_class',	associated_class],
				['related_component_1', related_component_1],
				['related_component_2', related_component_2],
				['campus',				str(section['campus'])],
				['enrollment_total',	section['enrollment_total']],
				['enrollment_capacity',	section['enrollment_capacity']],
				['waiting_total',		section['waiting_total']],
				['waiting_capacity',	section['waiting_capacity']],
				['held_with',			held_with]]


		self.insert_data(course_table, section_header_value_pairs)

		for reserve in section['reserves']:
			reserve_header_value_pairs = section_header_value_pairs[:]
			reserve_group = "None"
			reserve_capacity = 0
			reserve_total = 0
			if 'reserve_group' in reserve: 
				reserve_group = str(reserve['reserve_group'])
			if 'enrollment_capacity' in reserve: 
				reserve_capacity = str(reserve['enrollment_capacity'])
			if 'enrollment_total' in reserve: 
				reserve_total = str(reserve['enrollment_total'])
			reserve_header_value_pairs.append(
                ['reserve_group',		reserve_group])
			reserve_header_value_pairs.append(
                ['reserve_total',		reserve_total])
			reserve_header_value_pairs.append(
                ['reserve_capacity',	reserve_capacity])
			self.insert_data(course_table, reserve_header_value_pairs)

		time_schedule = course_table + section['section'].replace(" ", "") + \
        "_schedule"
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
	
	def is_opening(self, subject, catalog, section): #{{{
		"""Check whether a specified section is opening or not

		Args:
			subject (string): The suject of the course(e.g. 'CS')
			catalog (string): The catalog number of the course(e.g. '115')
			section (string): The section of the course(e.g. 'LEC 001')
		
		Return:
			bool: Ture if the course is open(not tba, closed or online)
				  False otherwise

		"""
		self.db.execute('SELECT is_tba, is_cancelled, is_closed FROM ' + \
				subject + catalog + section + '_schedule;')
		search_result = self.db.fetchall()
		# check if the section is tba or closed 
		for row in search_result:
			if (str(row[0]) == 'True' or str(row[1]) == 'True' or \
				str(row[2]) == 'True'):
				return False
		return True
		#}}}


	def course_opening(self, subject, catalog):
		self.update_course(subject, catalog)
		self.db.execute("SELECT name FROM sqlite_master" + \
				" WHERE type = 'table' AND name = '" + subject + catalog + "';")
		result = self.db.fetchall()
		if (result == []): return False
		else: return True

	def get_opening_sections(self, subject, catalog): #{{{
		"""Get all the opening sections of the specified course

		Args:
			subject (string): The subject of the course(e.g. 'CS')
			catalog (string): The catalog number of the course(e.g. '135')

		Returns:
			list: The result consists of several sublists(usually 2 or 3)
				  The sublists are all the open sections of different 
				  components of this course
				  e.g.
				  [['LEC 001', 'LEC 002'], ['TUT 101'], ['TST 201', 'TST 202']]

		"""
		self.update_course(subject, catalog)
		self.db.execute('SELECT section FROM ' + subject + catalog + \
				' ORDER BY section;')
		search_result = self.db.fetchall()

		# count the number of components(LEC, TUT etc.)
		result = []
		mapping = {}
		last_section_label = ''
		for row in search_result:
			section = str(row[0])
			if (section[:3] != last_section_label):
				last_section_label = section[:3]
				num = int(section[4:])
				mapping[section[:3]] = num;
				result.append([])
		idx = 0
		index = {}
		for key in sorted(mapping):
			index[key] = idx
			idx += 1
		# insert components names into the result list
		last_section_label = ''
		for row in search_result:
			section = str(row[0])
			no_space_section = section.replace(' ','')
			if (self.is_opening(subject, catalog, no_space_section)):
				idx = index[section[:3]] 
				if (section != last_section_label):
					last_section_label = section
					result[idx].append(section)
		result = filter(None, result)
		return result
		#}}}

	def new_get_related_sections(self, subject, catalog, section):#{{{
		"""Get all the possible combinations

		The combinations satisfies all the basic associations

		Args:
			subject (string): The subject of the course (e.g. 'CS')
			catalog (string): The catalog number of the course (e.g. '115')
			section (string): The section of the course (e.g. 'LEC 001')
		Returns:
			list: The list contains several sublists(usually 2 or 3)
				  The first sublist is the section specified 
				  The rest sublists are the valid associated sections of the 
				  section specified
				  e.g.
				  [['LEC 001'], ['TUT 101', 'TUT 102'], ['TST 201']]
		"""
		open_sections = self.get_opening_sections(subject, catalog)
		component_labels = []
		related_sections = []
		for component in open_sections:
			component_labels.append(component[0][:4])
			related_sections.append([])
		related_sections[int(section[-3])].append(section)
		self.db.execute("SELECT related_component_1," +  
				"related_component_2 FROM " + \
				 subject + catalog + " WHERE section = '" + section + "';")
		result = self.db.fetchone()
		is_free = [1, 1]
		if result == []: return result
		for related in result:
			if (str(related) == 'None'): continue
			idx = int(related[0])
			is_free[idx - 1] = 0
			name = component_labels[idx] + str(related) 
			if (name in open_sections[idx]):
				related_sections[idx].append(name)
		self.db.execute("SELECT associated_class FROM " + subject + catalog + \
				" WHERE section = '" + section + "';")
		associated = str(self.db.fetchone()[0])
		for i in range(len(component_labels)):
			if (related_sections[i] != [] or (i != 0 and not is_free[i - 1])): 
				continue
			self.db.execute("SELECT section FROM " + subject + catalog + \
					" WHERE section LIKE '" + component_labels[i] + \
					"%' AND associated_class = '" + associated + "';")
			result = self.db.fetchall()
			if result == []:
				self.db.execute("SELECT section FROM " + subject + catalog + \
						" WHERE section LIKE '" + component_labels[i] + \
						"%' AND associated_class = 'None';")
				result = self.db.fetchall()
			for section in result:
				name = str(section[0]).replace(' ','')
				if (self.is_opening(subject, catalog, name)):
					related_sections[i].append(str(section[0]))
		related_sections = filter(None, related_sections)		
		return related_sections
		#}}}
	
	def get_related_sections(self, subject, catalog, section): #{{{	
		# get open sections
		self.update_course(subject, catalog)
		open_sections_list = self.get_opening_sections(subject, catalog)
		catag_num = len(open_sections_list)
		self.db.execute('SELECT related_component_1, related_component_2' + \
				', associated_class' + ' FROM ' + subject + catalog + \
				" WHERE section = '" + section + "';")
		search_result = self.db.fetchall()
		if (search_result == []): return search_result
		search_result = search_result[0]
		associated_num = str(search_result[2])	
		result = [[] for i in range(catag_num)]
		result[0].append(section)
		is_free = [1, 1]
		# get manditory sections(related_component)
		for i in range (0, 2):
			if (search_result[i] != 'None'):
				self.db.execute('SELECT section FROM ' + subject + catalog + \
						" WHERE section LIKE '%" + search_result[i] + "%';")
				value = str(self.db.fetchall()[0][0])
				is_free[i] = 0
				idx = -1
				for j in range(1, catag_num):
					if (value in open_sections_list[j]): idx = j
				if (idx > 0):
					result[idx].append(value)
		for i in range(1, catag_num):
			if (result[i] == [] and is_free[i - 1] == 1):
				name = open_sections_list[i][0][:3]
				# get associated sections
				self.db.execute('SELECT section FROM ' + subject + catalog + \
						" WHERE section LIKE '%" + name + \
						"%' AND associated_class = '" + associated_num + "';")
				temp_result = self.db.fetchall()
				if (temp_result != []):
					for row in temp_result:
						if (str(row[0]) in open_sections_list[i]):
							result[i].append(str(row[0]))				
				# get free sections
				else:
					self.db.execute('SELECT section FROM ' + subject + \
					catalog + " WHERE section LIKE '%" + name + \
					"%' AND associated_class = 'None';")
					temp_result = self.db.fetchall()
					for row in temp_result:
						if (str(row[0]) in open_sections_list[i]):
							result[i].append(str(row[0]))
		result = filter(None, result) 
		return result				
	#}}}	

	def convert_weekday(self,weekdays): #{{{
		'''convert weekdays 'M', 'T', etc. into corresponding integers
		1, 2, etc.
		'''
		result = []
		i = 0
		while (i < len(weekdays)):
			day = weekdays[i]
			if (day == 'M'): result.append(1)
			elif (day == 'T' and i == len(weekdays) - 1): result.append(2)   # Do we need this?
			elif (day == 'T' and weekdays[i + 1] == 'h'):
				i += 1
				result.append(4)
			elif (day == 'T'): result.append(2)
			elif (day == 'W'): result.append(3)
			elif (day == 'F'): result.append(5)
			i += 1
		return result
		#}}}

	def get_time_schedule(self, subject, catalog, section): #{{{
		"""get time schedule of a class
		The result contains two components:
		[list_of_weekly_classes, list_of_one_time_classes], where
		Each weekly_class in list_of_weekly_classes is formatted as
		[list_of_weekdays, start_time, end_time]
		Each one_time_class in list_of_one_time_classes is formatted as
		[date, start_time, end_time]
		"""
		self.update_course(subject, catalog)
		no_space_section = section.replace(' ','')
		self.db.execute('SELECT start_date, end_date, start_time, end_time,' +\
		' weekdays FROM ' + subject + catalog + no_space_section + '_schedule;')
		search_result = self.db.fetchall()
		result = [[], []]
		for row in search_result:
			section_info = []
			start_date = str(row[0])
			end_date = str(row[1])
			start_time = str(row[2])
			start_time = datetime.datetime.strptime(start_time,'%H:%M').time()
			end_time = str(row[3])
			end_time = datetime.datetime.strptime(end_time,'%H:%M').time()
			weekdays = str(row[4])
			# get weekly sections
			if (start_date == 'None'):
				section_info.append(self.convert_weekday(weekdays))
				section_info.append(start_time)
				section_info.append(end_time)
				result[0].append(section_info)
			# get one-time sections
			else:
				year = (self.term % 1000) / 10 + 2000
				start_date = datetime.date(year, int(start_date[:2]), \
						int(start_date[-2:]))
				section_info.append(start_date)
				section_info.append(start_time)
				section_info.append(end_time)
				result[1].append(section_info)
		return result
		#}}}

	def get_instructors(self, subject, catalog, section): #{{{
		self.update_course(subject, catalog)
		self.db.execute('SELECT instructors FROM ' + subject + catalog + \
				section.replace(" ", "") + '_schedule;')
		return str(self.db.fetchone()[0])
		#}}}

	def get_reserve_info(self, subject, catalog, section): #{{{
		'''get reserve information about the class specified, in the
		form of [reserve_group, reserve_total, reserve_capacity].
			e.g. ["Year 1 Math Students", "20", "25"]
		'''
		self.update_course(subject, catalog)
		self.db.execute('SELECT reserve_group, reserve_total, reserve_capacity' + \
				' FROM ' + subject + catalog + " WHERE section = '" + section + "';")
		result = self.db.fetchall()
		return result[-1]        # it's just magic...
		#}}}

	def get_course_location(self, subject, catalog, section): #{{{
		'''get location information about the class specified, in the 
		form of [building, room]
			e.g. ["PAS", "2086"]
		'''
		self.update_course(subject, catalog)
		self.db.execute('SELECT building, room FROM ' + subject + catalog + \
				section.replace(" ", "") + "_schedule;")
		result = self.db.fetchall()
		return result[-1]
		#}}}
