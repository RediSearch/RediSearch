from operator import itemgetter

from unidata import unidata_split

Codepoints = list[str]  # ["0001", "0002", ...]
Weight = tuple[int, ...]  # (1234, 5678, ...) # when reweighted, it is a tuple with single number
Collection = list[tuple[Codepoints, Weight]]  # codepoints <-> weight


def coldata_strip(string: str) -> str:
	string = string.strip()
	return string


def coldata_split(string: str) -> list[str]:
	return unidata_split(string)


def weight2tuple(weight_str: str) -> Weight:
	'''weight is string like "aaaa.bbbb.cccc"'''
	return tuple([int(x, base=16) for x in weight_str.split('.')])


def collect_contractions(codepoints_file: str, contractions_file: str) -> tuple[Collection, Collection]:
	'''collect codepoints and contractions from file (title is misleading)
	in form of ([ "0001", "0002"], weight)'''

	def reweight_collection(collection: Collection):
		collection = sorted(collection, key=itemgetter(1))
		count = 0  # actually starts from weight 1, weight 0 is a special case for U+0000
		prev_weight = None
		for i, (codepoint, weight) in enumerate(collection):
			if weight != prev_weight or prev_weight is None:
				count += 1
			prev_weight = weight
			collection[i] = (codepoint, (count, ))

		return collection  # already sorted

	def split_collection(collection: Collection) -> tuple[Collection, Collection]:
		codepoints: Collection = []
		contractions: Collection = []
		for cps, weight in collection:
			if len(cps) > 1:
				contractions.append((cps, weight))
			else:
				codepoints.append((cps, weight))

		return codepoints, contractions

	combined: Collection = []
	for filename in (codepoints_file, contractions_file):
		for line in open(filename):
			tokens = coldata_split(line)
			points, weight = points2points(tokens[:-1]), weight2tuple(tokens[-1])
			combined.append((points, weight))

	combined = reweight_collection(combined)  # assign sequential weights
	codepoints, contractions = split_collection(combined)  # already sorted

	return codepoints, contractions


def points2points(points: list[str]) -> list[str]:
	'''0000 to 000000 i.e. any-byte codepoints to 6-byte codepoints
	this will make codepoints string representation usable w/o %06X formatting'''
	return ['%06X' % (int(point, base=16)) for point in points]


def find_weight(codepoints: Codepoints, collection: Collection) -> Weight | None:
	'''lookup weight in list of (codepoints, weight)
	works on both: contractions and codepoints'''
	for points, weight in collection:
		if points == codepoints:
			return weight
