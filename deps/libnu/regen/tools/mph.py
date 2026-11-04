# Easy Perfect Minimal Hashing
#
# Based on:
# By Steve Hanov. Released to the public domain.
# http://stevehanov.ca/blog/index.php?id=119
#
# Based on:
# Edward A. Fox, Lenwood S. Heath, Qi Fan Chen and Amjad M. Daoud,
# "Practical minimal perfect hash functions for large databases",
# CACM, 35(1):105-121
#
# also a good reference:
# Compress, Hash, and Displace algorithm by Djamal Belazzougui,
# Fabiano C. Botelho, and Martin Dietzfelbinger

import time

PRIME = 0x01000193  # no particular reason, because FVN is not currently used
INTERNAL_ENCODING = "UTF-8"  # internal encoding for COMBINED

G = list[int]
Values = list[tuple[str, int]]
Table = dict[str, tuple[str, int]]  # codepoint -> (codepoint, replacement)


def hash(delta: int, codepoint: str) -> int:
	'''calculates a distinct hash function for a given string. each value of
	the integer d results in a different hash value.'''

	if delta == 0:
		delta = PRIME

	c = int(codepoint, base=16)

	# it doesn't matter for MPH if it's FVN or not until G
	# is correctly filled, therefore simple XOR is enough to produce
	# required randomness while produced index fits into uint16_t.
	#
	# You can consider this as usage of Unicode codepoint as a hash
	# itself, but it need to depend on d to distibute values between
	# buckets

	return delta ^ c


def create_minimal_perfect_hash(table: Table) -> tuple[G, Values]:
	'''computes a minimal perfect hash table using the given python dictionary.
	it returns a tuple (G, V). G and V are both arrays. G contains the
	intermediate able of values needed to compute the index of the value in V.
	V contains the values of the dictionary.'''

	size = len(table)

	# Step 1: Place all of the keys into buckets
	buckets: list[list[str]] = [[] for _ in range(size)]
	G: list[int] = [0] * size
	values: list[tuple[str, int] | None] = [None] * size

	for key in table.keys():
		buckets[hash(0, key) % size].append(key)

	# Step 2: Sort the buckets and process the ones with the most items first.
	buckets.sort(key=len, reverse=True)

	b = 0  # This is to make linter happy
	for b in range(size):
		bucket = buckets[b]
		if len(bucket) <= 1:
			break

		d = 1
		item = 0
		slots: list[int] = []

		# Repeatedly try different values of d until we find a hash function
		# that places all items in the bucket into free slots
		while item < len(bucket):
			slot = hash(d, bucket[item]) % size
			if values[slot] is not None or slot in slots:
				d += 1
				item = 0
				slots = []
			else:
				slots.append(slot)
				item += 1

		G[hash(0, bucket[0]) % size] = d
		for i in range(len(bucket)):
			values[slots[i]] = table[bucket[i]]

	# only buckets with 1 item remain. process them more quickly by directly
	# placing them into a free slot. use a negative value of d to indicate
	# this.
	freelist: list[int] = []
	for i in range(size):
		if values[i] is None:
			freelist.append(i)

	for b in range(b, size):
		bucket = buckets[b]

		if len(bucket) == 0:
			break

		slot = freelist.pop()
		# we subtract one to ensure it's negative even if the zeroeth slot was
		# used.
		G[hash(0, bucket[0]) % size] = -slot - 1
		values[slot] = table[bucket[0]]

	# this is not really needed, but it is here to exclude None from values type
	filtered_values = [x for x in values if x is not None]
	return (G, filtered_values)


def perfect_hash_lookup(G: G, key: str) -> int:
	'''look up a value in the hash table, defined by G and V.'''

	d = G[hash(0, key) % len(G)]
	if d < 0:
		return -d - 1
	return hash(d, key) % len(G)


def non_non_character(character: str) -> bool:
	'''filter out non-characters from private area'''

	assert len(character) > 0
	return (ord(character) < 0xE000 or ord(character) > 0xF8FF)


def format_replacement(replacement: list[str]) -> str | None:
	'''produce C-source-ready decomposition string'''

	chars = ''.join(filter(non_non_character, [chr(int(x, base=16)) for x in replacement])).encode(INTERNAL_ENCODING)
	if len(chars) == 0:
		return
	formatted = ''.join(f'\\x{x:02X}' for x in chars)
	return formatted


def gen_header(tag: str, G: G, combined: str):
	'''print human-readable info regarding this hash-table'''

	print(f'''/* Automatically generated file (mph.py), {int(time.time())}
 *
 * Tag             : {tag}
 * Prime           : {PRIME:08X},
 * G size          : {len(G)},
 * Combined length : {len(combined) // 4},
 * Encoding        : {INTERNAL_ENCODING}
 */''')
	print('')


def gen_values(tag: str, V: Values, compact: bool = False):
	'''print values table'''

	boundary = 8

	print('/* codepoints */')
	print('/* clang-format off */')
	print('const ' + (compact and 'uint16_t' or 'uint32_t') + f' {tag}_VALUES_C[] = {{')
	for i, (codepoint, replacement) in enumerate(V):
		assert replacement is not None

		if i % boundary == 0:
			print('\t', end='')
		print(f'0x{int(codepoint, base=16):06X}, ', end='')
		if (i + 1) % boundary == 0:
			print('')

	print('};')
	print('/* clang-format on */')
	print('')

	boundary = 10

	print('/* indexes */')
	print('/* clang-format off */')
	print(f'const uint16_t {tag}_VALUES_I[] = {{')
	for i, (codepoint, replacement) in enumerate(V):
		assert replacement is not None

		if i % boundary == 0:
			print('\t', end='')
		print(f'0x{replacement:04X}, ', end='')
		if (i + 1) % boundary == 0:
			print('')
	print('};')
	print('/* clang-format on */')
	print('')


def gen_G(tag: str, G: G):
	'''print first hash table'''

	BOUNDARY = 12

	print('/* clang-format off */')
	print(f'const int16_t {tag}_G[] = {{')
	for i, x in enumerate(G):
		if i % BOUNDARY == 0:
			print('\t', end='')
		print(f'{x}, ', end='')
		if (i + 1) % BOUNDARY == 0:
			print('')
	print('};')
	print('/* clang-format on */')
	print('')
	print(f'const size_t {tag}_G_SIZE = sizeof({tag}_G) / sizeof(*{tag}_G);')
	print('')


def gen_combined(tag: str, combined: str):
	'''print combined encoded string'''

	BOUNDARY = 12 * 4

	def chunks(combined: str, n: int):
		for i in range(0, len(combined), n):
			yield combined[i:i + n]

	print('/* clang-format off */')
	print(f'const uint8_t {tag}_COMBINED[] = {{')
	for chunk in chunks(combined, BOUNDARY):
		parts = chunks(chunk, 4)
		parts_str = ', '.join(p.replace('\\', '0') for p in parts)  # replace \xYY with 0xYY
		print(f'\t{parts_str},')
	print('};')
	print('/* clang-format on */')
	print('')


def gen_includes():
	print('#include <stddef.h>')  # for size_t
	print('#include <stdint.h>')
	print('')
