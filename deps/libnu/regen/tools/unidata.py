#-*- coding: UTF-8


def unidata_strip(s):
	s = s.strip()

	if unidata_comment(s):
		return ''

	i = s.find('#')
	if i > 0:
		s = s[:i]

	s = s.strip()

	return s


def unidata_split(s):
	s = unidata_strip(s)
	return list(filter(bool, list(map(str.strip, s.split(' ')))))


def unidata_comment(line):
	return (not line or not line.strip() or line[0] == '#' or line[0] == '@')
