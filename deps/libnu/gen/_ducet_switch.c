/* Automatically generated file (contractions-toc), 1528636556
 *
 * Tag          : _nu_ducet
 * Contractions : 820
 */

#include <stdint.h>

#include "../udb.h"

const size_t _NU_DUCET_CONTRACTIONS = 820; /* contractions included in switch */
const size_t _NU_DUCET_CODEPOINTS = 20494; /* complementary codepoints number */

#define state_00AAB9 -838
#define state_000438 -826
#define state_0019B5 -749
#define state_001B09 -745
#define state_0019B7 -744
#define state_0019BA -737
#define state_00006C -712
#define state_0019B6 -686
#define state_00064A -684
#define state_000648 -674
#define state_00AABB -667
#define state_000418 -500
#define state_001B07 -482
#define state_001B05 -454
#define state_00AABC -394
#define state_000627 -391
#define state_000B92 -381
#define state_00004C -343
#define state_001B0D -252
#define state_001025 -217
#define state_000E40 -198
#define state_000E41 -197
#define state_000E42 -196
#define state_000E43 -195
#define state_000E44 -194
#define state_00AAB5 -171
#define state_00AAB6 -161
#define state_000EC1 -114
#define state_000EC0 -113
#define state_000EC3 -112
#define state_000EC2 -111
#define state_000EC4 -109
#define state_001B0B -59
#define state_001B11 -24

const int16_t _NU_DUCET_ROOTS_G[] = {
	0, -34, 0, 0, -33, -32, -31, -30, -29, -28, 2, -26, 
	-15, 0, 0, 3, 0, 0, -14, -13, -12, 8, 1, 8, 
	-10, 0, -7, -6, 5, 20, 4, -4, -2, 0, };

const size_t _NU_DUCET_ROOTS_G_SIZE = sizeof(_NU_DUCET_ROOTS_G) / sizeof(*_NU_DUCET_ROOTS_G);

/* codepoints */
const uint32_t _NU_DUCET_ROOTS_VALUES_C[] = {
	0x00004C, 0x001B0D, 0x000648, 0x000EC0, 0x00064A, 0x000E44, 0x001B0B, 0x000EC1, 
	0x000EC3, 0x001B05, 0x000E41, 0x000E43, 0x00006C, 0x000627, 0x0019B5, 0x001025, 
	0x001B07, 0x00AAB9, 0x000E40, 0x0019B7, 0x000E42, 0x00AABC, 0x001B09, 0x0019BA, 
	0x000EC2, 0x0019B6, 0x000B92, 0x000418, 0x00AABB, 0x000438, 0x00AAB5, 0x00AAB6, 
	0x001B11, 0x000EC4, };

/* indexes */
const uint16_t _NU_DUCET_ROOTS_VALUES_I[] = {
	0x0157, 0x00FC, 0x02A2, 0x0071, 0x02AC, 0x00C2, 0x003B, 0x0072, 0x0070, 0x01C6, 
	0x00C5, 0x00C3, 0x02C8, 0x0187, 0x02ED, 0x00D9, 0x01E2, 0x0346, 0x00C6, 0x02E8, 
	0x00C4, 0x018A, 0x02E9, 0x02E1, 0x006F, 0x02AE, 0x017D, 0x01F4, 0x029B, 0x033A, 
	0x00AB, 0x00A1, 0x0018, 0x006D, };

/* MPH lookup for root codepoints + binary search on balanced tree
 * for intermediate states */
int32_t _nu_ducet_weight_switch(uint32_t u, int32_t *w, void *context) {
	(void)(context);

	if (w == 0) { /*  first entry, root states */
		uint32_t state = nu_udb_lookup_value(u, _NU_DUCET_ROOTS_G, _NU_DUCET_ROOTS_G_SIZE,
			_NU_DUCET_ROOTS_VALUES_C, _NU_DUCET_ROOTS_VALUES_I);

		if (state != 0) {
			return -state; /* VALUES_I store negated (positive) states */
		}
	}

	if (w != 0) { /* re-entry, intermediate states */
		int32_t weight = *w;
		*w = 0;

		if (weight == state_00004C) {
			switch (u) {
			case 0x000387: return 0x0004A4; 
			case 0x0000B7: return 0x0004A4; 
			}

			*w = 1;
			return 0x00049B;
		}
		else if (weight < state_00004C) {
			if (weight == state_00064A) {
				switch (u) {
				case 0x000654: return 0x000CF3; 
				}

				*w = 1;
				return 0x001065;
			}
			else if (weight < state_00064A) {
				if (weight == state_0019B7) {
					switch (u) {
					case 0x0019A2: return 0x002029; 
					case 0x001999: return 0x001FFC; 
					case 0x001981: return 0x001F84; 
					case 0x00198E: return 0x001FC5; 
					case 0x001988: return 0x001FA7; 
					case 0x001994: return 0x001FE3; 
					case 0x0019A6: return 0x00203D; 
					case 0x00198A: return 0x001FB1; 
					case 0x001984: return 0x001F93; 
					case 0x00199D: return 0x002010; 
					case 0x001991: return 0x001FD4; 
					case 0x0019A3: return 0x00202E; 
					case 0x001980: return 0x001F7F; 
					case 0x00198D: return 0x001FC0; 
					case 0x001995: return 0x001FE8; 
					case 0x0019A7: return 0x002042; 
					case 0x00199A: return 0x002001; 
					case 0x0019AA: return 0x002051; 
					case 0x00199E: return 0x002015; 
					case 0x001992: return 0x001FD9; 
					case 0x001987: return 0x001FA2; 
					case 0x001996: return 0x001FED; 
					case 0x0019A0: return 0x00201F; 
					case 0x00199B: return 0x002006; 
					case 0x001983: return 0x001F8E; 
					case 0x0019AB: return 0x002056; 
					case 0x0019A4: return 0x002033; 
					case 0x00199F: return 0x00201A; 
					case 0x001993: return 0x001FDE; 
					case 0x00198C: return 0x001FBB; 
					case 0x001986: return 0x001F9D; 
					case 0x0019A8: return 0x002047; 
					case 0x001997: return 0x001FF2; 
					case 0x0019A1: return 0x002024; 
					case 0x00199C: return 0x00200B; 
					case 0x001998: return 0x001FF7; 
					case 0x001982: return 0x001F89; 
					case 0x00198F: return 0x001FCA; 
					case 0x001989: return 0x001FAC; 
					case 0x0019A5: return 0x002038; 
					case 0x00198B: return 0x001FB6; 
					case 0x001985: return 0x001F98; 
					case 0x0019A9: return 0x00204C; 
					case 0x001990: return 0x001FCF; 
					}

					*w = 1;
					return 0x00205F;
				}
				else if (weight < state_0019B7) {
					if (weight == state_0019B5) {
						switch (u) {
						case 0x0019A8: return 0x002045; 
						case 0x00199F: return 0x002018; 
						case 0x001993: return 0x001FDC; 
						case 0x0019AA: return 0x00204F; 
						case 0x0019A7: return 0x002040; 
						case 0x001982: return 0x001F87; 
						case 0x00198F: return 0x001FC8; 
						case 0x001997: return 0x001FF0; 
						case 0x00199C: return 0x002009; 
						case 0x0019A0: return 0x00201D; 
						case 0x00198B: return 0x001FB4; 
						case 0x0019A9: return 0x00204A; 
						case 0x001990: return 0x001FCD; 
						case 0x0019A4: return 0x002031; 
						case 0x001985: return 0x001F96; 
						case 0x001994: return 0x001FE1; 
						case 0x0019AB: return 0x002054; 
						case 0x001981: return 0x001F82; 
						case 0x0019A1: return 0x002022; 
						case 0x00198E: return 0x001FC3; 
						case 0x001998: return 0x001FF5; 
						case 0x00199D: return 0x00200E; 
						case 0x001991: return 0x001FD2; 
						case 0x0019A5: return 0x002036; 
						case 0x00198A: return 0x001FAF; 
						case 0x001984: return 0x001F91; 
						case 0x001995: return 0x001FE6; 
						case 0x001989: return 0x001FAA; 
						case 0x00199A: return 0x001FFF; 
						case 0x001980: return 0x001F7D; 
						case 0x00198D: return 0x001FBE; 
						case 0x001999: return 0x001FFA; 
						case 0x00199E: return 0x002013; 
						case 0x0019A2: return 0x002027; 
						case 0x001987: return 0x001FA0; 
						case 0x001992: return 0x001FD7; 
						case 0x001988: return 0x001FA5; 
						case 0x0019A6: return 0x00203B; 
						case 0x001983: return 0x001F8C; 
						case 0x001996: return 0x001FEB; 
						case 0x00199B: return 0x002004; 
						case 0x0019A3: return 0x00202C; 
						case 0x00198C: return 0x001FB9; 
						case 0x001986: return 0x001F9B; 
						}

						*w = 1;
						return 0x00205D;
					}
					else if (weight < state_0019B5) {
						if (weight == state_000438) {
							switch (u) {
							case 0x000306: return 0x0009D8; 
							}

							*w = 1;
							return 0x0009C8;
						}
						else if (weight < state_000438) {
							if (weight == state_00AAB9) {
								switch (u) {
								case 0x00AA92: return 0x001BC1; 
								case 0x00AAA5: return 0x001C33; 
								case 0x00AAAC: return 0x001C5D; 
								case 0x00AA8F: return 0x001BAF; 
								case 0x00AA82: return 0x001B61; 
								case 0x00AA9C: return 0x001BFD; 
								case 0x00AAA1: return 0x001C1B; 
								case 0x00AA97: return 0x001BDF; 
								case 0x00AAAD: return 0x001C63; 
								case 0x00AA86: return 0x001B79; 
								case 0x00AA93: return 0x001BC7; 
								case 0x00AA9D: return 0x001C03; 
								case 0x00AA8A: return 0x001B91; 
								case 0x00AAA6: return 0x001C39; 
								case 0x00AA94: return 0x001BCD; 
								case 0x00AA8E: return 0x001BA9; 
								case 0x00AAAE: return 0x001C69; 
								case 0x00AA81: return 0x001B5B; 
								case 0x00AAA2: return 0x001C21; 
								case 0x00AA90: return 0x001BB5; 
								case 0x00AA9E: return 0x001C09; 
								case 0x00AAAA: return 0x001C51; 
								case 0x00AA85: return 0x001B73; 
								case 0x00AA9A: return 0x001BF1; 
								case 0x00AAA7: return 0x001C3F; 
								case 0x00AA95: return 0x001BD3; 
								case 0x00AA89: return 0x001B8B; 
								case 0x00AA8D: return 0x001BA3; 
								case 0x00AA80: return 0x001B55; 
								case 0x00AA98: return 0x001BE5; 
								case 0x00AAA3: return 0x001C27; 
								case 0x00AA91: return 0x001BBB; 
								case 0x00AAA8: return 0x001C45; 
								case 0x00AAAF: return 0x001C6F; 
								case 0x00AA84: return 0x001B6D; 
								case 0x00AA8C: return 0x001B9D; 
								case 0x00AA9F: return 0x001C0F; 
								case 0x00AAA4: return 0x001C2D; 
								case 0x00AAAB: return 0x001C57; 
								case 0x00AA88: return 0x001B85; 
								case 0x00AA83: return 0x001B67; 
								case 0x00AA99: return 0x001BEB; 
								case 0x00AA9B: return 0x001BF7; 
								case 0x00AAA0: return 0x001C15; 
								case 0x00AA96: return 0x001BD9; 
								case 0x00AAA9: return 0x001C4B; 
								case 0x00AA87: return 0x001B7F; 
								case 0x00AA8B: return 0x001B97; 
								}

								*w = 1;
								return 0x001C75;
							}
						}
					}
					else { /* weight > state_0019B5 */
						if (weight == state_001B09) {
							switch (u) {
							case 0x001B35: return 0x0020E0; 
							}

							*w = 1;
							return 0x0020DF;
						}
					}
				}
				else { /* weight > state_0019B7 */
					if (weight == state_00006C) {
						switch (u) {
						case 0x0000B7: return 0x000493; 
						case 0x000387: return 0x000493; 
						}

						*w = 1;
						return 0x00048A;
					}
					else if (weight < state_00006C) {
						if (weight == state_0019BA) {
							switch (u) {
							case 0x00198F: return 0x001FCB; 
							case 0x0019A2: return 0x00202A; 
							case 0x001995: return 0x001FE9; 
							case 0x00199C: return 0x00200C; 
							case 0x001980: return 0x001F80; 
							case 0x001991: return 0x001FD5; 
							case 0x0019A7: return 0x002043; 
							case 0x001984: return 0x001F94; 
							case 0x00199D: return 0x002011; 
							case 0x00198A: return 0x001FB2; 
							case 0x0019A3: return 0x00202F; 
							case 0x001983: return 0x001F8F; 
							case 0x00198E: return 0x001FC6; 
							case 0x001996: return 0x001FEE; 
							case 0x0019A4: return 0x002034; 
							case 0x001987: return 0x001FA3; 
							case 0x00199E: return 0x002016; 
							case 0x001992: return 0x001FDA; 
							case 0x0019A0: return 0x002020; 
							case 0x00199A: return 0x002002; 
							case 0x001982: return 0x001F8A; 
							case 0x00198D: return 0x001FC1; 
							case 0x0019AA: return 0x002052; 
							case 0x001997: return 0x001FF3; 
							case 0x0019A5: return 0x002039; 
							case 0x001986: return 0x001F9E; 
							case 0x0019A8: return 0x002048; 
							case 0x001989: return 0x001FAD; 
							case 0x001993: return 0x001FDF; 
							case 0x00198C: return 0x001FBC; 
							case 0x0019A1: return 0x002025; 
							case 0x001998: return 0x001FF8; 
							case 0x00199F: return 0x00201B; 
							case 0x001994: return 0x001FE4; 
							case 0x00199B: return 0x002007; 
							case 0x001981: return 0x001F85; 
							case 0x0019A9: return 0x00204D; 
							case 0x0019AB: return 0x002057; 
							case 0x001988: return 0x001FA8; 
							case 0x001990: return 0x001FD0; 
							case 0x00198B: return 0x001FB7; 
							case 0x0019A6: return 0x00203E; 
							case 0x001999: return 0x001FFD; 
							case 0x001985: return 0x001F99; 
							}

							*w = 1;
							return 0x002062;
						}
					}
					else { /* weight > state_00006C */
						if (weight == state_0019B6) {
							switch (u) {
							case 0x001995: return 0x001FE7; 
							case 0x0019A3: return 0x00202D; 
							case 0x00199A: return 0x002000; 
							case 0x001980: return 0x001F7E; 
							case 0x00198D: return 0x001FBF; 
							case 0x001989: return 0x001FAB; 
							case 0x0019AA: return 0x002050; 
							case 0x0019A7: return 0x002041; 
							case 0x00199E: return 0x002014; 
							case 0x001990: return 0x001FCE; 
							case 0x001985: return 0x001F97; 
							case 0x001994: return 0x001FE2; 
							case 0x0019A2: return 0x002028; 
							case 0x001981: return 0x001F83; 
							case 0x00198E: return 0x001FC4; 
							case 0x0019A6: return 0x00203C; 
							case 0x00199D: return 0x00200F; 
							case 0x00198A: return 0x001FB0; 
							case 0x001986: return 0x001F9C; 
							case 0x001993: return 0x001FDD; 
							case 0x0019A1: return 0x002023; 
							case 0x001982: return 0x001F88; 
							case 0x00198F: return 0x001FC9; 
							case 0x001997: return 0x001FF1; 
							case 0x0019A5: return 0x002037; 
							case 0x00199C: return 0x00200A; 
							case 0x00198B: return 0x001FB5; 
							case 0x001987: return 0x001FA1; 
							case 0x0019A9: return 0x00204B; 
							case 0x001992: return 0x001FD8; 
							case 0x0019A0: return 0x00201E; 
							case 0x001999: return 0x001FFB; 
							case 0x001983: return 0x001F8D; 
							case 0x001996: return 0x001FEC; 
							case 0x0019A4: return 0x002032; 
							case 0x00199B: return 0x002005; 
							case 0x00198C: return 0x001FBA; 
							case 0x001988: return 0x001FA6; 
							case 0x0019AB: return 0x002055; 
							case 0x0019A8: return 0x002046; 
							case 0x00199F: return 0x002019; 
							case 0x001991: return 0x001FD3; 
							case 0x001998: return 0x001FF6; 
							case 0x001984: return 0x001F92; 
							}

							*w = 1;
							return 0x00205E;
						}
					}
				}
			}
			else { /* weight > state_00064A */
				if (weight == state_001B05) {
					switch (u) {
					case 0x001B35: return 0x0020DC; 
					}

					*w = 1;
					return 0x0020DB;
				}
				else if (weight < state_001B05) {
					if (weight == state_000418) {
						switch (u) {
						case 0x000306: return 0x0009D9; 
						}

						*w = 1;
						return 0x0009CC;
					}
					else if (weight < state_000418) {
						if (weight == state_00AABB) {
							switch (u) {
							case 0x00AAA0: return 0x001C16; 
							case 0x00AA8D: return 0x001BA4; 
							case 0x00AA81: return 0x001B5C; 
							case 0x00AA99: return 0x001BEC; 
							case 0x00AA90: return 0x001BB6; 
							case 0x00AAAF: return 0x001C70; 
							case 0x00AA9D: return 0x001C04; 
							case 0x00AA85: return 0x001B74; 
							case 0x00AA8A: return 0x001B92; 
							case 0x00AAAB: return 0x001C58; 
							case 0x00AA89: return 0x001B8C; 
							case 0x00AAA7: return 0x001C40; 
							case 0x00AA8E: return 0x001BAA; 
							case 0x00AA82: return 0x001B62; 
							case 0x00AA98: return 0x001BE6; 
							case 0x00AA97: return 0x001BE0; 
							case 0x00AAAE: return 0x001C6A; 
							case 0x00AAA3: return 0x001C28; 
							case 0x00AA86: return 0x001B7A; 
							case 0x00AA8B: return 0x001B98; 
							case 0x00AA93: return 0x001BC8; 
							case 0x00AAAA: return 0x001C52; 
							case 0x00AAA6: return 0x001C3A; 
							case 0x00AA8F: return 0x001BB0; 
							case 0x00AA83: return 0x001B68; 
							case 0x00AA9C: return 0x001BFE; 
							case 0x00AA96: return 0x001BDA; 
							case 0x00AAAD: return 0x001C64; 
							case 0x00AAA2: return 0x001C22; 
							case 0x00AA87: return 0x001B80; 
							case 0x00AAA9: return 0x001C4C; 
							case 0x00AA8C: return 0x001B9E; 
							case 0x00AA92: return 0x001BC2; 
							case 0x00AA9F: return 0x001C10; 
							case 0x00AAA5: return 0x001C34; 
							case 0x00AA9B: return 0x001BF8; 
							case 0x00AA95: return 0x001BD4; 
							case 0x00AAA1: return 0x001C1C; 
							case 0x00AA80: return 0x001B56; 
							case 0x00AAA8: return 0x001C46; 
							case 0x00AA91: return 0x001BBC; 
							case 0x00AA9E: return 0x001C0A; 
							case 0x00AA84: return 0x001B6E; 
							case 0x00AAA4: return 0x001C2E; 
							case 0x00AAAC: return 0x001C5E; 
							case 0x00AA9A: return 0x001BF2; 
							case 0x00AA94: return 0x001BCE; 
							case 0x00AA88: return 0x001B86; 
							}

							*w = 1;
							return 0x001C77;
						}
						else if (weight < state_00AABB) {
							if (weight == state_000648) {
								switch (u) {
								case 0x000654: return 0x000CEA; 
								}

								*w = 1;
								return 0x00103D;
							}
						}
					}
					else { /* weight > state_000418 */
						if (weight == state_001B07) {
							switch (u) {
							case 0x001B35: return 0x0020DE; 
							}

							*w = 1;
							return 0x0020DD;
						}
					}
				}
				else { /* weight > state_001B05 */
					if (weight == state_000627) {
						switch (u) {
						case 0x000653: return 0x000CE0; 
						case 0x000655: return 0x000CED; 
						case 0x000654: return 0x000CE3; 
						}

						*w = 1;
						return 0x000D1D;
					}
					else if (weight < state_000627) {
						if (weight == state_00AABC) {
							switch (u) {
							case 0x00AA95: return 0x001BD5; 
							case 0x00AAA1: return 0x001C1D; 
							case 0x00AA84: return 0x001B6F; 
							case 0x00AA98: return 0x001BE7; 
							case 0x00AA91: return 0x001BBD; 
							case 0x00AAAC: return 0x001C5F; 
							case 0x00AA9E: return 0x001C0B; 
							case 0x00AA88: return 0x001B87; 
							case 0x00AAA6: return 0x001C3B; 
							case 0x00AA8D: return 0x001BA5; 
							case 0x00AA9A: return 0x001BF3; 
							case 0x00AA96: return 0x001BDB; 
							case 0x00AAA2: return 0x001C23; 
							case 0x00AA83: return 0x001B69; 
							case 0x00AA99: return 0x001BED; 
							case 0x00AA92: return 0x001BC3; 
							case 0x00AAAD: return 0x001C65; 
							case 0x00AA9F: return 0x001C11; 
							case 0x00AA87: return 0x001B81; 
							case 0x00AAA7: return 0x001C41; 
							case 0x00AA8C: return 0x001B9F; 
							case 0x00AA9B: return 0x001BF9; 
							case 0x00AA97: return 0x001BE1; 
							case 0x00AAA3: return 0x001C29; 
							case 0x00AA82: return 0x001B63; 
							case 0x00AA93: return 0x001BC9; 
							case 0x00AAAE: return 0x001C6B; 
							case 0x00AA86: return 0x001B7B; 
							case 0x00AAA8: return 0x001C47; 
							case 0x00AA8B: return 0x001B99; 
							case 0x00AAAA: return 0x001C53; 
							case 0x00AA9C: return 0x001BFF; 
							case 0x00AAA4: return 0x001C2F; 
							case 0x00AA8F: return 0x001BB1; 
							case 0x00AA81: return 0x001B5D; 
							case 0x00AA94: return 0x001BCF; 
							case 0x00AAAF: return 0x001C71; 
							case 0x00AAA0: return 0x001C17; 
							case 0x00AA85: return 0x001B75; 
							case 0x00AAA9: return 0x001C4D; 
							case 0x00AA8A: return 0x001B93; 
							case 0x00AA90: return 0x001BB7; 
							case 0x00AAAB: return 0x001C59; 
							case 0x00AA9D: return 0x001C05; 
							case 0x00AA89: return 0x001B8D; 
							case 0x00AAA5: return 0x001C35; 
							case 0x00AA8E: return 0x001BAB; 
							case 0x00AA80: return 0x001B57; 
							}

							*w = 1;
							return 0x001C78;
						}
					}
					else { /* weight > state_000627 */
						if (weight == state_000B92) {
							switch (u) {
							case 0x000BD7: return 0x001465; 
							}

							*w = 1;
							return 0x001463;
						}
					}
				}
			}
		}
		else { /* weight > state_00004C */
			if (weight == state_00AAB6) {
				switch (u) {
				case 0x00AA9D: return 0x001C02; 
				case 0x00AA87: return 0x001B7E; 
				case 0x00AA8A: return 0x001B90; 
				case 0x00AAA9: return 0x001C4A; 
				case 0x00AAAD: return 0x001C62; 
				case 0x00AA92: return 0x001BC0; 
				case 0x00AAA0: return 0x001C14; 
				case 0x00AA99: return 0x001BEA; 
				case 0x00AA83: return 0x001B66; 
				case 0x00AA96: return 0x001BD8; 
				case 0x00AA8F: return 0x001BAE; 
				case 0x00AAA4: return 0x001C2C; 
				case 0x00AA9C: return 0x001BFC; 
				case 0x00AA88: return 0x001B84; 
				case 0x00AA8B: return 0x001B96; 
				case 0x00AAA8: return 0x001C44; 
				case 0x00AAAC: return 0x001C5C; 
				case 0x00AA91: return 0x001BBA; 
				case 0x00AA98: return 0x001BE4; 
				case 0x00AA84: return 0x001B6C; 
				case 0x00AA95: return 0x001BD2; 
				case 0x00AAA3: return 0x001C26; 
				case 0x00AA80: return 0x001B54; 
				case 0x00AA9B: return 0x001BF6; 
				case 0x00AA89: return 0x001B8A; 
				case 0x00AA8C: return 0x001B9C; 
				case 0x00AAA7: return 0x001C3E; 
				case 0x00AAAB: return 0x001C56; 
				case 0x00AA90: return 0x001BB4; 
				case 0x00AA9F: return 0x001C0E; 
				case 0x00AA85: return 0x001B72; 
				case 0x00AAAF: return 0x001C6E; 
				case 0x00AA94: return 0x001BCC; 
				case 0x00AAA2: return 0x001C20; 
				case 0x00AA81: return 0x001B5A; 
				case 0x00AA9A: return 0x001BF0; 
				case 0x00AA8D: return 0x001BA2; 
				case 0x00AAA6: return 0x001C38; 
				case 0x00AAAA: return 0x001C50; 
				case 0x00AA9E: return 0x001C08; 
				case 0x00AA86: return 0x001B78; 
				case 0x00AAAE: return 0x001C68; 
				case 0x00AA93: return 0x001BC6; 
				case 0x00AAA1: return 0x001C1A; 
				case 0x00AA82: return 0x001B60; 
				case 0x00AA97: return 0x001BDE; 
				case 0x00AA8E: return 0x001BA8; 
				case 0x00AAA5: return 0x001C32; 
				}

				*w = 1;
				return 0x001C74;
			}
			else if (weight < state_00AAB6) {
				if (weight == state_000E42) {
					switch (u) {
					case 0x000E1C: return 0x001A15; 
					case 0x000E16: return 0x0019F1; 
					case 0x000E24: return 0x001A45; 
					case 0x000E07: return 0x001997; 
					case 0x000E0C: return 0x0019B5; 
					case 0x000E12: return 0x0019D9; 
					case 0x000E1F: return 0x001A27; 
					case 0x000E2D: return 0x001A7B; 
					case 0x000E21: return 0x001A33; 
					case 0x000E1B: return 0x001A0F; 
					case 0x000E15: return 0x0019EB; 
					case 0x000E25: return 0x001A4B; 
					case 0x000E2A: return 0x001A69; 
					case 0x000E23: return 0x001A3F; 
					case 0x000E11: return 0x0019D3; 
					case 0x000E1E: return 0x001A21; 
					case 0x000E2E: return 0x001A81; 
					case 0x000E04: return 0x001985; 
					case 0x000E1A: return 0x001A09; 
					case 0x000E14: return 0x0019E5; 
					case 0x000E08: return 0x00199D; 
					case 0x000E0D: return 0x0019BB; 
					case 0x000E01: return 0x001973; 
					case 0x000E19: return 0x001A03; 
					case 0x000E28: return 0x001A5D; 
					case 0x000E10: return 0x0019CD; 
					case 0x000E26: return 0x001A51; 
					case 0x000E1D: return 0x001A1B; 
					case 0x000E2B: return 0x001A6F; 
					case 0x000E05: return 0x00198B; 
					case 0x000E0A: return 0x0019A9; 
					case 0x000E09: return 0x0019A3; 
					case 0x000E0E: return 0x0019C1; 
					case 0x000E02: return 0x001979; 
					case 0x000E18: return 0x0019FD; 
					case 0x000E17: return 0x0019F7; 
					case 0x000E27: return 0x001A57; 
					case 0x000E2C: return 0x001A75; 
					case 0x000E06: return 0x001991; 
					case 0x000E0B: return 0x0019AF; 
					case 0x000E13: return 0x0019DF; 
					case 0x000E29: return 0x001A63; 
					case 0x000E20: return 0x001A2D; 
					case 0x000E22: return 0x001A39; 
					case 0x000E0F: return 0x0019C7; 
					case 0x000E03: return 0x00197F; 
					}

					*w = 1;
					return 0x001A8A;
				}
				else if (weight < state_000E42) {
					if (weight == state_000E40) {
						switch (u) {
						case 0x000E04: return 0x001983; 
						case 0x000E27: return 0x001A55; 
						case 0x000E11: return 0x0019D1; 
						case 0x000E2C: return 0x001A73; 
						case 0x000E1E: return 0x001A1F; 
						case 0x000E18: return 0x0019FB; 
						case 0x000E0D: return 0x0019B9; 
						case 0x000E01: return 0x001971; 
						case 0x000E1A: return 0x001A07; 
						case 0x000E14: return 0x0019E3; 
						case 0x000E05: return 0x001989; 
						case 0x000E0A: return 0x0019A7; 
						case 0x000E20: return 0x001A2B; 
						case 0x000E10: return 0x0019CB; 
						case 0x000E29: return 0x001A61; 
						case 0x000E1D: return 0x001A19; 
						case 0x000E0E: return 0x0019BF; 
						case 0x000E24: return 0x001A43; 
						case 0x000E17: return 0x0019F5; 
						case 0x000E02: return 0x001977; 
						case 0x000E2D: return 0x001A79; 
						case 0x000E21: return 0x001A31; 
						case 0x000E13: return 0x0019DD; 
						case 0x000E22: return 0x001A37; 
						case 0x000E06: return 0x00198F; 
						case 0x000E0B: return 0x0019AD; 
						case 0x000E25: return 0x001A49; 
						case 0x000E2A: return 0x001A67; 
						case 0x000E1C: return 0x001A13; 
						case 0x000E16: return 0x0019EF; 
						case 0x000E0F: return 0x0019C5; 
						case 0x000E03: return 0x00197D; 
						case 0x000E2E: return 0x001A7F; 
						case 0x000E08: return 0x00199B; 
						case 0x000E12: return 0x0019D7; 
						case 0x000E1F: return 0x001A25; 
						case 0x000E07: return 0x001995; 
						case 0x000E0C: return 0x0019B3; 
						case 0x000E26: return 0x001A4F; 
						case 0x000E2B: return 0x001A6D; 
						case 0x000E1B: return 0x001A0D; 
						case 0x000E19: return 0x001A01; 
						case 0x000E23: return 0x001A3D; 
						case 0x000E28: return 0x001A5B; 
						case 0x000E09: return 0x0019A1; 
						case 0x000E15: return 0x0019E9; 
						}

						*w = 1;
						return 0x001A88;
					}
					else if (weight < state_000E40) {
						if (weight == state_001025) {
							switch (u) {
							case 0x00102E: return 0x001ECF; 
							}

							*w = 1;
							return 0x001ECE;
						}
						else if (weight < state_001025) {
							if (weight == state_001B0D) {
								switch (u) {
								case 0x001B35: return 0x0020E4; 
								}

								*w = 1;
								return 0x0020E3;
							}
						}
					}
					else { /* weight > state_000E40 */
						if (weight == state_000E41) {
							switch (u) {
							case 0x000E2C: return 0x001A74; 
							case 0x000E26: return 0x001A50; 
							case 0x000E14: return 0x0019E4; 
							case 0x000E01: return 0x001972; 
							case 0x000E0E: return 0x0019C0; 
							case 0x000E08: return 0x00199C; 
							case 0x000E1D: return 0x001A1A; 
							case 0x000E11: return 0x0019D2; 
							case 0x000E0A: return 0x0019A8; 
							case 0x000E04: return 0x001984; 
							case 0x000E29: return 0x001A62; 
							case 0x000E2B: return 0x001A6E; 
							case 0x000E25: return 0x001A4A; 
							case 0x000E15: return 0x0019EA; 
							case 0x000E1A: return 0x001A08; 
							case 0x000E0D: return 0x0019BA; 
							case 0x000E21: return 0x001A32; 
							case 0x000E2E: return 0x001A80; 
							case 0x000E1E: return 0x001A20; 
							case 0x000E23: return 0x001A3E; 
							case 0x000E07: return 0x001996; 
							case 0x000E2A: return 0x001A68; 
							case 0x000E24: return 0x001A44; 
							case 0x000E12: return 0x0019D8; 
							case 0x000E03: return 0x00197E; 
							case 0x000E20: return 0x001A2C; 
							case 0x000E16: return 0x0019F0; 
							case 0x000E28: return 0x001A5C; 
							case 0x000E2D: return 0x001A7A; 
							case 0x000E1B: return 0x001A0E; 
							case 0x000E0C: return 0x0019B4; 
							case 0x000E06: return 0x001990; 
							case 0x000E1F: return 0x001A26; 
							case 0x000E13: return 0x0019DE; 
							case 0x000E18: return 0x0019FC; 
							case 0x000E02: return 0x001978; 
							case 0x000E0F: return 0x0019C6; 
							case 0x000E27: return 0x001A56; 
							case 0x000E17: return 0x0019F6; 
							case 0x000E1C: return 0x001A14; 
							case 0x000E0B: return 0x0019AE; 
							case 0x000E09: return 0x0019A2; 
							case 0x000E22: return 0x001A38; 
							case 0x000E10: return 0x0019CC; 
							case 0x000E19: return 0x001A02; 
							case 0x000E05: return 0x00198A; 
							}

							*w = 1;
							return 0x001A89;
						}
					}
				}
				else { /* weight > state_000E42 */
					if (weight == state_000E44) {
						switch (u) {
						case 0x000E1E: return 0x001A23; 
						case 0x000E10: return 0x0019CF; 
						case 0x000E08: return 0x00199F; 
						case 0x000E01: return 0x001975; 
						case 0x000E28: return 0x001A5F; 
						case 0x000E0E: return 0x0019C3; 
						case 0x000E14: return 0x0019E7; 
						case 0x000E2B: return 0x001A71; 
						case 0x000E27: return 0x001A59; 
						case 0x000E29: return 0x001A65; 
						case 0x000E0A: return 0x0019AB; 
						case 0x000E18: return 0x0019FF; 
						case 0x000E22: return 0x001A3B; 
						case 0x000E1D: return 0x001A1D; 
						case 0x000E13: return 0x0019E1; 
						case 0x000E09: return 0x0019A5; 
						case 0x000E06: return 0x001993; 
						case 0x000E17: return 0x0019F9; 
						case 0x000E1C: return 0x001A17; 
						case 0x000E2C: return 0x001A77; 
						case 0x000E02: return 0x00197B; 
						case 0x000E0F: return 0x0019C9; 
						case 0x000E12: return 0x0019DB; 
						case 0x000E24: return 0x001A47; 
						case 0x000E0B: return 0x0019B1; 
						case 0x000E07: return 0x001999; 
						case 0x000E16: return 0x0019F3; 
						case 0x000E20: return 0x001A2F; 
						case 0x000E23: return 0x001A41; 
						case 0x000E1B: return 0x001A11; 
						case 0x000E2D: return 0x001A7D; 
						case 0x000E03: return 0x001981; 
						case 0x000E1F: return 0x001A29; 
						case 0x000E25: return 0x001A4D; 
						case 0x000E0C: return 0x0019B7; 
						case 0x000E04: return 0x001987; 
						case 0x000E11: return 0x0019D5; 
						case 0x000E21: return 0x001A35; 
						case 0x000E2E: return 0x001A83; 
						case 0x000E0D: return 0x0019BD; 
						case 0x000E15: return 0x0019ED; 
						case 0x000E1A: return 0x001A0B; 
						case 0x000E2A: return 0x001A6B; 
						case 0x000E26: return 0x001A53; 
						case 0x000E05: return 0x00198D; 
						case 0x000E19: return 0x001A05; 
						}

						*w = 1;
						return 0x001A8C;
					}
					else if (weight < state_000E44) {
						if (weight == state_000E43) {
							switch (u) {
							case 0x000E02: return 0x00197A; 
							case 0x000E22: return 0x001A3A; 
							case 0x000E13: return 0x0019E0; 
							case 0x000E25: return 0x001A4C; 
							case 0x000E2A: return 0x001A6A; 
							case 0x000E06: return 0x001992; 
							case 0x000E0B: return 0x0019B0; 
							case 0x000E28: return 0x001A5E; 
							case 0x000E1C: return 0x001A16; 
							case 0x000E2E: return 0x001A82; 
							case 0x000E20: return 0x001A2E; 
							case 0x000E0F: return 0x0019C8; 
							case 0x000E01: return 0x001974; 
							case 0x000E14: return 0x0019E6; 
							case 0x000E24: return 0x001A46; 
							case 0x000E05: return 0x00198C; 
							case 0x000E0A: return 0x0019AA; 
							case 0x000E10: return 0x0019CE; 
							case 0x000E1D: return 0x001A1C; 
							case 0x000E2D: return 0x001A7C; 
							case 0x000E09: return 0x0019A4; 
							case 0x000E0E: return 0x0019C2; 
							case 0x000E15: return 0x0019EC; 
							case 0x000E27: return 0x001A58; 
							case 0x000E2C: return 0x001A76; 
							case 0x000E04: return 0x001986; 
							case 0x000E18: return 0x0019FE; 
							case 0x000E11: return 0x0019D4; 
							case 0x000E1E: return 0x001A22; 
							case 0x000E08: return 0x00199E; 
							case 0x000E23: return 0x001A40; 
							case 0x000E0D: return 0x0019BC; 
							case 0x000E1A: return 0x001A0A; 
							case 0x000E16: return 0x0019F2; 
							case 0x000E26: return 0x001A52; 
							case 0x000E2B: return 0x001A70; 
							case 0x000E03: return 0x001980; 
							case 0x000E19: return 0x001A04; 
							case 0x000E29: return 0x001A64; 
							case 0x000E12: return 0x0019DA; 
							case 0x000E1F: return 0x001A28; 
							case 0x000E07: return 0x001998; 
							case 0x000E0C: return 0x0019B6; 
							case 0x000E1B: return 0x001A10; 
							case 0x000E17: return 0x0019F8; 
							case 0x000E21: return 0x001A34; 
							}

							*w = 1;
							return 0x001A8B;
						}
					}
					else { /* weight > state_000E44 */
						if (weight == state_00AAB5) {
							switch (u) {
							case 0x00AAA8: return 0x001C43; 
							case 0x00AA9F: return 0x001C0D; 
							case 0x00AA93: return 0x001BC5; 
							case 0x00AAAA: return 0x001C4F; 
							case 0x00AAA7: return 0x001C3D; 
							case 0x00AA82: return 0x001B5F; 
							case 0x00AA8F: return 0x001BAD; 
							case 0x00AA97: return 0x001BDD; 
							case 0x00AAAE: return 0x001C67; 
							case 0x00AA9C: return 0x001BFB; 
							case 0x00AAA0: return 0x001C13; 
							case 0x00AA8B: return 0x001B95; 
							case 0x00AAA9: return 0x001C49; 
							case 0x00AA90: return 0x001BB3; 
							case 0x00AAA4: return 0x001C2B; 
							case 0x00AA85: return 0x001B71; 
							case 0x00AA94: return 0x001BCB; 
							case 0x00AAAB: return 0x001C55; 
							case 0x00AA81: return 0x001B59; 
							case 0x00AAA1: return 0x001C19; 
							case 0x00AA8E: return 0x001BA7; 
							case 0x00AA98: return 0x001BE3; 
							case 0x00AAAF: return 0x001C6D; 
							case 0x00AA9D: return 0x001C01; 
							case 0x00AA91: return 0x001BB9; 
							case 0x00AAA5: return 0x001C31; 
							case 0x00AA8A: return 0x001B8F; 
							case 0x00AA84: return 0x001B6B; 
							case 0x00AA95: return 0x001BD1; 
							case 0x00AA89: return 0x001B89; 
							case 0x00AAAC: return 0x001C5B; 
							case 0x00AA9A: return 0x001BEF; 
							case 0x00AA80: return 0x001B53; 
							case 0x00AA8D: return 0x001BA1; 
							case 0x00AA99: return 0x001BE9; 
							case 0x00AA9E: return 0x001C07; 
							case 0x00AAA2: return 0x001C1F; 
							case 0x00AA87: return 0x001B7D; 
							case 0x00AA92: return 0x001BBF; 
							case 0x00AA88: return 0x001B83; 
							case 0x00AAA6: return 0x001C37; 
							case 0x00AA83: return 0x001B65; 
							case 0x00AA96: return 0x001BD7; 
							case 0x00AAAD: return 0x001C61; 
							case 0x00AA9B: return 0x001BF5; 
							case 0x00AAA3: return 0x001C25; 
							case 0x00AA8C: return 0x001B9B; 
							case 0x00AA86: return 0x001B77; 
							}

							*w = 1;
							return 0x001C73;
						}
					}
				}
			}
			else { /* weight > state_00AAB6 */
				if (weight == state_000EC2) {
					switch (u) {
					case 0x000E82: return 0x001A9D; 
					case 0x000E9B: return 0x001AF1; 
					case 0x000EDD: return 0x001B39; 
					case 0x000EAD: return 0x001B3F; 
					case 0x000E9F: return 0x001B09; 
					case 0x000EAA: return 0x001AB5; 
					case 0x000E81: return 0x001A97; 
					case 0x000E9C: return 0x001AF7; 
					case 0x000E94: return 0x001ACD; 
					case 0x000EDC: return 0x001B33; 
					case 0x000EAE: return 0x001B45; 
					case 0x000EA1: return 0x001B0F; 
					case 0x000E84: return 0x001AA3; 
					case 0x000EA5: return 0x001B21; 
					case 0x000EAB: return 0x001B2D; 
					case 0x000E95: return 0x001AD3; 
					case 0x000EA2: return 0x001B15; 
					case 0x000E99: return 0x001AE5; 
					case 0x000E8A: return 0x001ABB; 
					case 0x000EDF: return 0x001AC1; 
					case 0x000E88: return 0x001AAF; 
					case 0x000E9D: return 0x001AFD; 
					case 0x000E87: return 0x001AA9; 
					case 0x000E96: return 0x001AD9; 
					case 0x000E8D: return 0x001AC7; 
					case 0x000E9A: return 0x001AEB; 
					case 0x000EA3: return 0x001B1B; 
					case 0x000EDE: return 0x001A91; 
					case 0x000E9E: return 0x001B03; 
					case 0x000EA7: return 0x001B27; 
					case 0x000E97: return 0x001ADF; 
					}

					*w = 1;
					return 0x001B4F;
				}
				else if (weight < state_000EC2) {
					if (weight == state_000EC0) {
						switch (u) {
						case 0x000E84: return 0x001AA1; 
						case 0x000E9D: return 0x001AFB; 
						case 0x000E8A: return 0x001AB9; 
						case 0x000EAB: return 0x001B2B; 
						case 0x000E88: return 0x001AAD; 
						case 0x000E99: return 0x001AE3; 
						case 0x000E96: return 0x001AD7; 
						case 0x000EA2: return 0x001B13; 
						case 0x000E87: return 0x001AA7; 
						case 0x000E9E: return 0x001B01; 
						case 0x000EDE: return 0x001A8F; 
						case 0x000E9A: return 0x001AE9; 
						case 0x000EA7: return 0x001B25; 
						case 0x000E82: return 0x001A9B; 
						case 0x000E8D: return 0x001AC5; 
						case 0x000E97: return 0x001ADD; 
						case 0x000EA3: return 0x001B19; 
						case 0x000EAD: return 0x001B3D; 
						case 0x000EDD: return 0x001B37; 
						case 0x000E9F: return 0x001B07; 
						case 0x000E94: return 0x001ACB; 
						case 0x000E9B: return 0x001AEF; 
						case 0x000EAE: return 0x001B43; 
						case 0x000E81: return 0x001A95; 
						case 0x000EAA: return 0x001AB3; 
						case 0x000EA5: return 0x001B1F; 
						case 0x000EDC: return 0x001B31; 
						case 0x000E95: return 0x001AD1; 
						case 0x000E9C: return 0x001AF5; 
						case 0x000EA1: return 0x001B0D; 
						case 0x000EDF: return 0x001ABF; 
						}

						*w = 1;
						return 0x001B4D;
					}
					else if (weight < state_000EC0) {
						if (weight == state_000EC1) {
							switch (u) {
							case 0x000EAD: return 0x001B3E; 
							case 0x000E97: return 0x001ADE; 
							case 0x000EA1: return 0x001B0E; 
							case 0x000E9C: return 0x001AF6; 
							case 0x000E82: return 0x001A9C; 
							case 0x000EDE: return 0x001A90; 
							case 0x000EA5: return 0x001B20; 
							case 0x000E87: return 0x001AA8; 
							case 0x000E96: return 0x001AD8; 
							case 0x000E9B: return 0x001AF0; 
							case 0x000E99: return 0x001AE4; 
							case 0x000E88: return 0x001AAE; 
							case 0x000EAB: return 0x001B2C; 
							case 0x000E9F: return 0x001B08; 
							case 0x000E84: return 0x001AA2; 
							case 0x000EDF: return 0x001AC0; 
							case 0x000EA3: return 0x001B1A; 
							case 0x000E8D: return 0x001AC6; 
							case 0x000E95: return 0x001AD2; 
							case 0x000EDC: return 0x001B32; 
							case 0x000EA7: return 0x001B26; 
							case 0x000E9A: return 0x001AEA; 
							case 0x000EAA: return 0x001AB4; 
							case 0x000E9E: return 0x001B02; 
							case 0x000EA2: return 0x001B14; 
							case 0x000E81: return 0x001A96; 
							case 0x000EAE: return 0x001B44; 
							case 0x000E94: return 0x001ACC; 
							case 0x000E8A: return 0x001ABA; 
							case 0x000EDD: return 0x001B38; 
							case 0x000E9D: return 0x001AFC; 
							}

							*w = 1;
							return 0x001B4E;
						}
					}
					else { /* weight > state_000EC0 */
						if (weight == state_000EC3) {
							switch (u) {
							case 0x000E96: return 0x001ADA; 
							case 0x000EAE: return 0x001B46; 
							case 0x000E9C: return 0x001AF8; 
							case 0x000EA1: return 0x001B10; 
							case 0x000EDE: return 0x001A92; 
							case 0x000EA5: return 0x001B22; 
							case 0x000E84: return 0x001AA4; 
							case 0x000E95: return 0x001AD4; 
							case 0x000EAD: return 0x001B40; 
							case 0x000E9B: return 0x001AF2; 
							case 0x000E99: return 0x001AE6; 
							case 0x000EDF: return 0x001AC2; 
							case 0x000E9F: return 0x001B0A; 
							case 0x000E94: return 0x001ACE; 
							case 0x000E88: return 0x001AB0; 
							case 0x000EDC: return 0x001B34; 
							case 0x000E81: return 0x001A98; 
							case 0x000E9A: return 0x001AEC; 
							case 0x000E8D: return 0x001AC8; 
							case 0x000E9E: return 0x001B04; 
							case 0x000EA3: return 0x001B1C; 
							case 0x000EA7: return 0x001B28; 
							case 0x000EAB: return 0x001B2E; 
							case 0x000E82: return 0x001A9E; 
							case 0x000E97: return 0x001AE0; 
							case 0x000E9D: return 0x001AFE; 
							case 0x000EA2: return 0x001B16; 
							case 0x000E87: return 0x001AAA; 
							case 0x000E8A: return 0x001ABC; 
							case 0x000EDD: return 0x001B3A; 
							case 0x000EAA: return 0x001AB6; 
							}

							*w = 1;
							return 0x001B50;
						}
					}
				}
				else { /* weight > state_000EC2 */
					if (weight == state_001B0B) {
						switch (u) {
						case 0x001B35: return 0x0020E2; 
						}

						*w = 1;
						return 0x0020E1;
					}
					else if (weight < state_001B0B) {
						if (weight == state_000EC4) {
							switch (u) {
							case 0x000EDD: return 0x001B3B; 
							case 0x000E9F: return 0x001B0B; 
							case 0x000EAA: return 0x001AB7; 
							case 0x000EA7: return 0x001B29; 
							case 0x000E82: return 0x001A9F; 
							case 0x000E97: return 0x001AE1; 
							case 0x000EDC: return 0x001B35; 
							case 0x000EAE: return 0x001B47; 
							case 0x000E9C: return 0x001AF9; 
							case 0x000E94: return 0x001ACF; 
							case 0x000EAB: return 0x001B2F; 
							case 0x000E81: return 0x001A99; 
							case 0x000EA1: return 0x001B11; 
							case 0x000EDF: return 0x001AC3; 
							case 0x000E9D: return 0x001AFF; 
							case 0x000EA5: return 0x001B23; 
							case 0x000E8A: return 0x001ABD; 
							case 0x000E84: return 0x001AA5; 
							case 0x000E95: return 0x001AD5; 
							case 0x000E9A: return 0x001AED; 
							case 0x000E8D: return 0x001AC9; 
							case 0x000E99: return 0x001AE7; 
							case 0x000E9E: return 0x001B05; 
							case 0x000EA2: return 0x001B17; 
							case 0x000E87: return 0x001AAB; 
							case 0x000EDE: return 0x001A93; 
							case 0x000E88: return 0x001AB1; 
							case 0x000E96: return 0x001ADB; 
							case 0x000EAD: return 0x001B41; 
							case 0x000E9B: return 0x001AF3; 
							case 0x000EA3: return 0x001B1D; 
							}

							*w = 1;
							return 0x001B51;
						}
					}
					else { /* weight > state_001B0B */
						if (weight == state_001B11) {
							switch (u) {
							case 0x001B35: return 0x0020E8; 
							}

							*w = 1;
							return 0x0020E7;
						}
					}
				}
			}
		}
	}

	return 0;
}
