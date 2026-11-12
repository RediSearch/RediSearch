def unidata_strip(string: str) -> str:
	string = string.strip()

	if unidata_comment(string):
		return ''

	i = string.find('#')
	if i > 0:
		string = string[:i]

	string = string.strip()

	return string


def unidata_split(string: str) -> list[str]:
	stripped = unidata_strip(string)
	return [x for x in map(str.strip, stripped.split(' ')) if x]


def unidata_comment(line: str) -> bool:
	return (not line or not line.strip() or line[0] == '#' or line[0] == '@')
