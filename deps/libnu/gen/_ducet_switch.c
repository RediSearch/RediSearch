/* Automatically generated file (contractions-toc), 1466614860
 *
 * Tag          : _nu_ducet
 * Contractions : 820
 */

#include <stdint.h>

#include "../udb.h"

const size_t _NU_DUCET_CONTRACTIONS = 820; /* contractions included in switch */
const size_t _NU_DUCET_CODEPOINTS = 19581; /* complementary codepoints number */

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
			case 0x000387: return 0x000456; 
			case 0x0000B7: return 0x000456; 
			}

			*w = 1;
			return 0x00044D;
		}
		else if (weight < state_00004C) {
			if (weight == state_00064A) {
				switch (u) {
				case 0x000654: return 0x000C71; 
				}

				*w = 1;
				return 0x000FE3;
			}
			else if (weight < state_00064A) {
				if (weight == state_0019B7) {
					switch (u) {
					case 0x0019A2: return 0x001E82; 
					case 0x001999: return 0x001E55; 
					case 0x001981: return 0x001DDD; 
					case 0x00198E: return 0x001E1E; 
					case 0x001988: return 0x001E00; 
					case 0x001994: return 0x001E3C; 
					case 0x0019A6: return 0x001E96; 
					case 0x00198A: return 0x001E0A; 
					case 0x001984: return 0x001DEC; 
					case 0x00199D: return 0x001E69; 
					case 0x001991: return 0x001E2D; 
					case 0x0019A3: return 0x001E87; 
					case 0x001980: return 0x001DD8; 
					case 0x00198D: return 0x001E19; 
					case 0x001995: return 0x001E41; 
					case 0x0019A7: return 0x001E9B; 
					case 0x00199A: return 0x001E5A; 
					case 0x0019AA: return 0x001EAA; 
					case 0x00199E: return 0x001E6E; 
					case 0x001992: return 0x001E32; 
					case 0x001987: return 0x001DFB; 
					case 0x001996: return 0x001E46; 
					case 0x0019A0: return 0x001E78; 
					case 0x00199B: return 0x001E5F; 
					case 0x001983: return 0x001DE7; 
					case 0x0019AB: return 0x001EAF; 
					case 0x0019A4: return 0x001E8C; 
					case 0x00199F: return 0x001E73; 
					case 0x001993: return 0x001E37; 
					case 0x00198C: return 0x001E14; 
					case 0x001986: return 0x001DF6; 
					case 0x0019A8: return 0x001EA0; 
					case 0x001997: return 0x001E4B; 
					case 0x0019A1: return 0x001E7D; 
					case 0x00199C: return 0x001E64; 
					case 0x001998: return 0x001E50; 
					case 0x001982: return 0x001DE2; 
					case 0x00198F: return 0x001E23; 
					case 0x001989: return 0x001E05; 
					case 0x0019A5: return 0x001E91; 
					case 0x00198B: return 0x001E0F; 
					case 0x001985: return 0x001DF1; 
					case 0x0019A9: return 0x001EA5; 
					case 0x001990: return 0x001E28; 
					}

					*w = 1;
					return 0x001EB8;
				}
				else if (weight < state_0019B7) {
					if (weight == state_0019B5) {
						switch (u) {
						case 0x0019A8: return 0x001E9E; 
						case 0x00199F: return 0x001E71; 
						case 0x001993: return 0x001E35; 
						case 0x0019AA: return 0x001EA8; 
						case 0x0019A7: return 0x001E99; 
						case 0x001982: return 0x001DE0; 
						case 0x00198F: return 0x001E21; 
						case 0x001997: return 0x001E49; 
						case 0x00199C: return 0x001E62; 
						case 0x0019A0: return 0x001E76; 
						case 0x00198B: return 0x001E0D; 
						case 0x0019A9: return 0x001EA3; 
						case 0x001990: return 0x001E26; 
						case 0x0019A4: return 0x001E8A; 
						case 0x001985: return 0x001DEF; 
						case 0x001994: return 0x001E3A; 
						case 0x0019AB: return 0x001EAD; 
						case 0x001981: return 0x001DDB; 
						case 0x0019A1: return 0x001E7B; 
						case 0x00198E: return 0x001E1C; 
						case 0x001998: return 0x001E4E; 
						case 0x00199D: return 0x001E67; 
						case 0x001991: return 0x001E2B; 
						case 0x0019A5: return 0x001E8F; 
						case 0x00198A: return 0x001E08; 
						case 0x001984: return 0x001DEA; 
						case 0x001995: return 0x001E3F; 
						case 0x001989: return 0x001E03; 
						case 0x00199A: return 0x001E58; 
						case 0x001980: return 0x001DD6; 
						case 0x00198D: return 0x001E17; 
						case 0x001999: return 0x001E53; 
						case 0x00199E: return 0x001E6C; 
						case 0x0019A2: return 0x001E80; 
						case 0x001987: return 0x001DF9; 
						case 0x001992: return 0x001E30; 
						case 0x001988: return 0x001DFE; 
						case 0x0019A6: return 0x001E94; 
						case 0x001983: return 0x001DE5; 
						case 0x001996: return 0x001E44; 
						case 0x00199B: return 0x001E5D; 
						case 0x0019A3: return 0x001E85; 
						case 0x00198C: return 0x001E12; 
						case 0x001986: return 0x001DF4; 
						}

						*w = 1;
						return 0x001EB6;
					}
					else if (weight < state_0019B5) {
						if (weight == state_000438) {
							switch (u) {
							case 0x000306: return 0x000987; 
							}

							*w = 1;
							return 0x000977;
						}
						else if (weight < state_000438) {
							if (weight == state_00AAB9) {
								switch (u) {
								case 0x00AA92: return 0x001AAA; 
								case 0x00AAA5: return 0x001B1C; 
								case 0x00AAAC: return 0x001B46; 
								case 0x00AA8F: return 0x001A98; 
								case 0x00AA82: return 0x001A4A; 
								case 0x00AA9C: return 0x001AE6; 
								case 0x00AAA1: return 0x001B04; 
								case 0x00AA97: return 0x001AC8; 
								case 0x00AAAD: return 0x001B4C; 
								case 0x00AA86: return 0x001A62; 
								case 0x00AA93: return 0x001AB0; 
								case 0x00AA9D: return 0x001AEC; 
								case 0x00AA8A: return 0x001A7A; 
								case 0x00AAA6: return 0x001B22; 
								case 0x00AA94: return 0x001AB6; 
								case 0x00AA8E: return 0x001A92; 
								case 0x00AAAE: return 0x001B52; 
								case 0x00AA81: return 0x001A44; 
								case 0x00AAA2: return 0x001B0A; 
								case 0x00AA90: return 0x001A9E; 
								case 0x00AA9E: return 0x001AF2; 
								case 0x00AAAA: return 0x001B3A; 
								case 0x00AA85: return 0x001A5C; 
								case 0x00AA9A: return 0x001ADA; 
								case 0x00AAA7: return 0x001B28; 
								case 0x00AA95: return 0x001ABC; 
								case 0x00AA89: return 0x001A74; 
								case 0x00AA8D: return 0x001A8C; 
								case 0x00AA80: return 0x001A3E; 
								case 0x00AA98: return 0x001ACE; 
								case 0x00AAA3: return 0x001B10; 
								case 0x00AA91: return 0x001AA4; 
								case 0x00AAA8: return 0x001B2E; 
								case 0x00AAAF: return 0x001B58; 
								case 0x00AA84: return 0x001A56; 
								case 0x00AA8C: return 0x001A86; 
								case 0x00AA9F: return 0x001AF8; 
								case 0x00AAA4: return 0x001B16; 
								case 0x00AAAB: return 0x001B40; 
								case 0x00AA88: return 0x001A6E; 
								case 0x00AA83: return 0x001A50; 
								case 0x00AA99: return 0x001AD4; 
								case 0x00AA9B: return 0x001AE0; 
								case 0x00AAA0: return 0x001AFE; 
								case 0x00AA96: return 0x001AC2; 
								case 0x00AAA9: return 0x001B34; 
								case 0x00AA87: return 0x001A68; 
								case 0x00AA8B: return 0x001A80; 
								}

								*w = 1;
								return 0x001B5E;
							}
						}
					}
					else { /* weight > state_0019B5 */
						if (weight == state_001B09) {
							switch (u) {
							case 0x001B35: return 0x001F39; 
							}

							*w = 1;
							return 0x001F38;
						}
					}
				}
				else { /* weight > state_0019B7 */
					if (weight == state_00006C) {
						switch (u) {
						case 0x0000B7: return 0x000445; 
						case 0x000387: return 0x000445; 
						}

						*w = 1;
						return 0x00043C;
					}
					else if (weight < state_00006C) {
						if (weight == state_0019BA) {
							switch (u) {
							case 0x00198F: return 0x001E24; 
							case 0x0019A2: return 0x001E83; 
							case 0x001995: return 0x001E42; 
							case 0x00199C: return 0x001E65; 
							case 0x001980: return 0x001DD9; 
							case 0x001991: return 0x001E2E; 
							case 0x0019A7: return 0x001E9C; 
							case 0x001984: return 0x001DED; 
							case 0x00199D: return 0x001E6A; 
							case 0x00198A: return 0x001E0B; 
							case 0x0019A3: return 0x001E88; 
							case 0x001983: return 0x001DE8; 
							case 0x00198E: return 0x001E1F; 
							case 0x001996: return 0x001E47; 
							case 0x0019A4: return 0x001E8D; 
							case 0x001987: return 0x001DFC; 
							case 0x00199E: return 0x001E6F; 
							case 0x001992: return 0x001E33; 
							case 0x0019A0: return 0x001E79; 
							case 0x00199A: return 0x001E5B; 
							case 0x001982: return 0x001DE3; 
							case 0x00198D: return 0x001E1A; 
							case 0x0019AA: return 0x001EAB; 
							case 0x001997: return 0x001E4C; 
							case 0x0019A5: return 0x001E92; 
							case 0x001986: return 0x001DF7; 
							case 0x0019A8: return 0x001EA1; 
							case 0x001989: return 0x001E06; 
							case 0x001993: return 0x001E38; 
							case 0x00198C: return 0x001E15; 
							case 0x0019A1: return 0x001E7E; 
							case 0x001998: return 0x001E51; 
							case 0x00199F: return 0x001E74; 
							case 0x001994: return 0x001E3D; 
							case 0x00199B: return 0x001E60; 
							case 0x001981: return 0x001DDE; 
							case 0x0019A9: return 0x001EA6; 
							case 0x0019AB: return 0x001EB0; 
							case 0x001988: return 0x001E01; 
							case 0x001990: return 0x001E29; 
							case 0x00198B: return 0x001E10; 
							case 0x0019A6: return 0x001E97; 
							case 0x001999: return 0x001E56; 
							case 0x001985: return 0x001DF2; 
							}

							*w = 1;
							return 0x001EBB;
						}
					}
					else { /* weight > state_00006C */
						if (weight == state_0019B6) {
							switch (u) {
							case 0x001995: return 0x001E40; 
							case 0x0019A3: return 0x001E86; 
							case 0x00199A: return 0x001E59; 
							case 0x001980: return 0x001DD7; 
							case 0x00198D: return 0x001E18; 
							case 0x001989: return 0x001E04; 
							case 0x0019AA: return 0x001EA9; 
							case 0x0019A7: return 0x001E9A; 
							case 0x00199E: return 0x001E6D; 
							case 0x001990: return 0x001E27; 
							case 0x001985: return 0x001DF0; 
							case 0x001994: return 0x001E3B; 
							case 0x0019A2: return 0x001E81; 
							case 0x001981: return 0x001DDC; 
							case 0x00198E: return 0x001E1D; 
							case 0x0019A6: return 0x001E95; 
							case 0x00199D: return 0x001E68; 
							case 0x00198A: return 0x001E09; 
							case 0x001986: return 0x001DF5; 
							case 0x001993: return 0x001E36; 
							case 0x0019A1: return 0x001E7C; 
							case 0x001982: return 0x001DE1; 
							case 0x00198F: return 0x001E22; 
							case 0x001997: return 0x001E4A; 
							case 0x0019A5: return 0x001E90; 
							case 0x00199C: return 0x001E63; 
							case 0x00198B: return 0x001E0E; 
							case 0x001987: return 0x001DFA; 
							case 0x0019A9: return 0x001EA4; 
							case 0x001992: return 0x001E31; 
							case 0x0019A0: return 0x001E77; 
							case 0x001999: return 0x001E54; 
							case 0x001983: return 0x001DE6; 
							case 0x001996: return 0x001E45; 
							case 0x0019A4: return 0x001E8B; 
							case 0x00199B: return 0x001E5E; 
							case 0x00198C: return 0x001E13; 
							case 0x001988: return 0x001DFF; 
							case 0x0019AB: return 0x001EAE; 
							case 0x0019A8: return 0x001E9F; 
							case 0x00199F: return 0x001E72; 
							case 0x001991: return 0x001E2C; 
							case 0x001998: return 0x001E4F; 
							case 0x001984: return 0x001DEB; 
							}

							*w = 1;
							return 0x001EB7;
						}
					}
				}
			}
			else { /* weight > state_00064A */
				if (weight == state_001B05) {
					switch (u) {
					case 0x001B35: return 0x001F35; 
					}

					*w = 1;
					return 0x001F34;
				}
				else if (weight < state_001B05) {
					if (weight == state_000418) {
						switch (u) {
						case 0x000306: return 0x000988; 
						}

						*w = 1;
						return 0x00097B;
					}
					else if (weight < state_000418) {
						if (weight == state_00AABB) {
							switch (u) {
							case 0x00AAA0: return 0x001AFF; 
							case 0x00AA8D: return 0x001A8D; 
							case 0x00AA81: return 0x001A45; 
							case 0x00AA99: return 0x001AD5; 
							case 0x00AA90: return 0x001A9F; 
							case 0x00AAAF: return 0x001B59; 
							case 0x00AA9D: return 0x001AED; 
							case 0x00AA85: return 0x001A5D; 
							case 0x00AA8A: return 0x001A7B; 
							case 0x00AAAB: return 0x001B41; 
							case 0x00AA89: return 0x001A75; 
							case 0x00AAA7: return 0x001B29; 
							case 0x00AA8E: return 0x001A93; 
							case 0x00AA82: return 0x001A4B; 
							case 0x00AA98: return 0x001ACF; 
							case 0x00AA97: return 0x001AC9; 
							case 0x00AAAE: return 0x001B53; 
							case 0x00AAA3: return 0x001B11; 
							case 0x00AA86: return 0x001A63; 
							case 0x00AA8B: return 0x001A81; 
							case 0x00AA93: return 0x001AB1; 
							case 0x00AAAA: return 0x001B3B; 
							case 0x00AAA6: return 0x001B23; 
							case 0x00AA8F: return 0x001A99; 
							case 0x00AA83: return 0x001A51; 
							case 0x00AA9C: return 0x001AE7; 
							case 0x00AA96: return 0x001AC3; 
							case 0x00AAAD: return 0x001B4D; 
							case 0x00AAA2: return 0x001B0B; 
							case 0x00AA87: return 0x001A69; 
							case 0x00AAA9: return 0x001B35; 
							case 0x00AA8C: return 0x001A87; 
							case 0x00AA92: return 0x001AAB; 
							case 0x00AA9F: return 0x001AF9; 
							case 0x00AAA5: return 0x001B1D; 
							case 0x00AA9B: return 0x001AE1; 
							case 0x00AA95: return 0x001ABD; 
							case 0x00AAA1: return 0x001B05; 
							case 0x00AA80: return 0x001A3F; 
							case 0x00AAA8: return 0x001B2F; 
							case 0x00AA91: return 0x001AA5; 
							case 0x00AA9E: return 0x001AF3; 
							case 0x00AA84: return 0x001A57; 
							case 0x00AAA4: return 0x001B17; 
							case 0x00AAAC: return 0x001B47; 
							case 0x00AA9A: return 0x001ADB; 
							case 0x00AA94: return 0x001AB7; 
							case 0x00AA88: return 0x001A6F; 
							}

							*w = 1;
							return 0x001B60;
						}
						else if (weight < state_00AABB) {
							if (weight == state_000648) {
								switch (u) {
								case 0x000654: return 0x000C68; 
								}

								*w = 1;
								return 0x000FBB;
							}
						}
					}
					else { /* weight > state_000418 */
						if (weight == state_001B07) {
							switch (u) {
							case 0x001B35: return 0x001F37; 
							}

							*w = 1;
							return 0x001F36;
						}
					}
				}
				else { /* weight > state_001B05 */
					if (weight == state_000627) {
						switch (u) {
						case 0x000653: return 0x000C5E; 
						case 0x000655: return 0x000C6B; 
						case 0x000654: return 0x000C61; 
						}

						*w = 1;
						return 0x000C9B;
					}
					else if (weight < state_000627) {
						if (weight == state_00AABC) {
							switch (u) {
							case 0x00AA95: return 0x001ABE; 
							case 0x00AAA1: return 0x001B06; 
							case 0x00AA84: return 0x001A58; 
							case 0x00AA98: return 0x001AD0; 
							case 0x00AA91: return 0x001AA6; 
							case 0x00AAAC: return 0x001B48; 
							case 0x00AA9E: return 0x001AF4; 
							case 0x00AA88: return 0x001A70; 
							case 0x00AAA6: return 0x001B24; 
							case 0x00AA8D: return 0x001A8E; 
							case 0x00AA9A: return 0x001ADC; 
							case 0x00AA96: return 0x001AC4; 
							case 0x00AAA2: return 0x001B0C; 
							case 0x00AA83: return 0x001A52; 
							case 0x00AA99: return 0x001AD6; 
							case 0x00AA92: return 0x001AAC; 
							case 0x00AAAD: return 0x001B4E; 
							case 0x00AA9F: return 0x001AFA; 
							case 0x00AA87: return 0x001A6A; 
							case 0x00AAA7: return 0x001B2A; 
							case 0x00AA8C: return 0x001A88; 
							case 0x00AA9B: return 0x001AE2; 
							case 0x00AA97: return 0x001ACA; 
							case 0x00AAA3: return 0x001B12; 
							case 0x00AA82: return 0x001A4C; 
							case 0x00AA93: return 0x001AB2; 
							case 0x00AAAE: return 0x001B54; 
							case 0x00AA86: return 0x001A64; 
							case 0x00AAA8: return 0x001B30; 
							case 0x00AA8B: return 0x001A82; 
							case 0x00AAAA: return 0x001B3C; 
							case 0x00AA9C: return 0x001AE8; 
							case 0x00AAA4: return 0x001B18; 
							case 0x00AA8F: return 0x001A9A; 
							case 0x00AA81: return 0x001A46; 
							case 0x00AA94: return 0x001AB8; 
							case 0x00AAAF: return 0x001B5A; 
							case 0x00AAA0: return 0x001B00; 
							case 0x00AA85: return 0x001A5E; 
							case 0x00AAA9: return 0x001B36; 
							case 0x00AA8A: return 0x001A7C; 
							case 0x00AA90: return 0x001AA0; 
							case 0x00AAAB: return 0x001B42; 
							case 0x00AA9D: return 0x001AEE; 
							case 0x00AA89: return 0x001A76; 
							case 0x00AAA5: return 0x001B1E; 
							case 0x00AA8E: return 0x001A94; 
							case 0x00AA80: return 0x001A40; 
							}

							*w = 1;
							return 0x001B61;
						}
					}
					else { /* weight > state_000627 */
						if (weight == state_000B92) {
							switch (u) {
							case 0x000BD7: return 0x0013D6; 
							}

							*w = 1;
							return 0x0013D4;
						}
					}
				}
			}
		}
		else { /* weight > state_00004C */
			if (weight == state_00AAB6) {
				switch (u) {
				case 0x00AA9D: return 0x001AEB; 
				case 0x00AA87: return 0x001A67; 
				case 0x00AA8A: return 0x001A79; 
				case 0x00AAA9: return 0x001B33; 
				case 0x00AAAD: return 0x001B4B; 
				case 0x00AA92: return 0x001AA9; 
				case 0x00AAA0: return 0x001AFD; 
				case 0x00AA99: return 0x001AD3; 
				case 0x00AA83: return 0x001A4F; 
				case 0x00AA96: return 0x001AC1; 
				case 0x00AA8F: return 0x001A97; 
				case 0x00AAA4: return 0x001B15; 
				case 0x00AA9C: return 0x001AE5; 
				case 0x00AA88: return 0x001A6D; 
				case 0x00AA8B: return 0x001A7F; 
				case 0x00AAA8: return 0x001B2D; 
				case 0x00AAAC: return 0x001B45; 
				case 0x00AA91: return 0x001AA3; 
				case 0x00AA98: return 0x001ACD; 
				case 0x00AA84: return 0x001A55; 
				case 0x00AA95: return 0x001ABB; 
				case 0x00AAA3: return 0x001B0F; 
				case 0x00AA80: return 0x001A3D; 
				case 0x00AA9B: return 0x001ADF; 
				case 0x00AA89: return 0x001A73; 
				case 0x00AA8C: return 0x001A85; 
				case 0x00AAA7: return 0x001B27; 
				case 0x00AAAB: return 0x001B3F; 
				case 0x00AA90: return 0x001A9D; 
				case 0x00AA9F: return 0x001AF7; 
				case 0x00AA85: return 0x001A5B; 
				case 0x00AAAF: return 0x001B57; 
				case 0x00AA94: return 0x001AB5; 
				case 0x00AAA2: return 0x001B09; 
				case 0x00AA81: return 0x001A43; 
				case 0x00AA9A: return 0x001AD9; 
				case 0x00AA8D: return 0x001A8B; 
				case 0x00AAA6: return 0x001B21; 
				case 0x00AAAA: return 0x001B39; 
				case 0x00AA9E: return 0x001AF1; 
				case 0x00AA86: return 0x001A61; 
				case 0x00AAAE: return 0x001B51; 
				case 0x00AA93: return 0x001AAF; 
				case 0x00AAA1: return 0x001B03; 
				case 0x00AA82: return 0x001A49; 
				case 0x00AA97: return 0x001AC7; 
				case 0x00AA8E: return 0x001A91; 
				case 0x00AAA5: return 0x001B1B; 
				}

				*w = 1;
				return 0x001B5D;
			}
			else if (weight < state_00AAB6) {
				if (weight == state_000E42) {
					switch (u) {
					case 0x000E1C: return 0x0018FE; 
					case 0x000E16: return 0x0018DA; 
					case 0x000E24: return 0x00192E; 
					case 0x000E07: return 0x001880; 
					case 0x000E0C: return 0x00189E; 
					case 0x000E12: return 0x0018C2; 
					case 0x000E1F: return 0x001910; 
					case 0x000E2D: return 0x001964; 
					case 0x000E21: return 0x00191C; 
					case 0x000E1B: return 0x0018F8; 
					case 0x000E15: return 0x0018D4; 
					case 0x000E25: return 0x001934; 
					case 0x000E2A: return 0x001952; 
					case 0x000E23: return 0x001928; 
					case 0x000E11: return 0x0018BC; 
					case 0x000E1E: return 0x00190A; 
					case 0x000E2E: return 0x00196A; 
					case 0x000E04: return 0x00186E; 
					case 0x000E1A: return 0x0018F2; 
					case 0x000E14: return 0x0018CE; 
					case 0x000E08: return 0x001886; 
					case 0x000E0D: return 0x0018A4; 
					case 0x000E01: return 0x00185C; 
					case 0x000E19: return 0x0018EC; 
					case 0x000E28: return 0x001946; 
					case 0x000E10: return 0x0018B6; 
					case 0x000E26: return 0x00193A; 
					case 0x000E1D: return 0x001904; 
					case 0x000E2B: return 0x001958; 
					case 0x000E05: return 0x001874; 
					case 0x000E0A: return 0x001892; 
					case 0x000E09: return 0x00188C; 
					case 0x000E0E: return 0x0018AA; 
					case 0x000E02: return 0x001862; 
					case 0x000E18: return 0x0018E6; 
					case 0x000E17: return 0x0018E0; 
					case 0x000E27: return 0x001940; 
					case 0x000E2C: return 0x00195E; 
					case 0x000E06: return 0x00187A; 
					case 0x000E0B: return 0x001898; 
					case 0x000E13: return 0x0018C8; 
					case 0x000E29: return 0x00194C; 
					case 0x000E20: return 0x001916; 
					case 0x000E22: return 0x001922; 
					case 0x000E0F: return 0x0018B0; 
					case 0x000E03: return 0x001868; 
					}

					*w = 1;
					return 0x001973;
				}
				else if (weight < state_000E42) {
					if (weight == state_000E40) {
						switch (u) {
						case 0x000E04: return 0x00186C; 
						case 0x000E27: return 0x00193E; 
						case 0x000E11: return 0x0018BA; 
						case 0x000E2C: return 0x00195C; 
						case 0x000E1E: return 0x001908; 
						case 0x000E18: return 0x0018E4; 
						case 0x000E0D: return 0x0018A2; 
						case 0x000E01: return 0x00185A; 
						case 0x000E1A: return 0x0018F0; 
						case 0x000E14: return 0x0018CC; 
						case 0x000E05: return 0x001872; 
						case 0x000E0A: return 0x001890; 
						case 0x000E20: return 0x001914; 
						case 0x000E10: return 0x0018B4; 
						case 0x000E29: return 0x00194A; 
						case 0x000E1D: return 0x001902; 
						case 0x000E0E: return 0x0018A8; 
						case 0x000E24: return 0x00192C; 
						case 0x000E17: return 0x0018DE; 
						case 0x000E02: return 0x001860; 
						case 0x000E2D: return 0x001962; 
						case 0x000E21: return 0x00191A; 
						case 0x000E13: return 0x0018C6; 
						case 0x000E22: return 0x001920; 
						case 0x000E06: return 0x001878; 
						case 0x000E0B: return 0x001896; 
						case 0x000E25: return 0x001932; 
						case 0x000E2A: return 0x001950; 
						case 0x000E1C: return 0x0018FC; 
						case 0x000E16: return 0x0018D8; 
						case 0x000E0F: return 0x0018AE; 
						case 0x000E03: return 0x001866; 
						case 0x000E2E: return 0x001968; 
						case 0x000E08: return 0x001884; 
						case 0x000E12: return 0x0018C0; 
						case 0x000E1F: return 0x00190E; 
						case 0x000E07: return 0x00187E; 
						case 0x000E0C: return 0x00189C; 
						case 0x000E26: return 0x001938; 
						case 0x000E2B: return 0x001956; 
						case 0x000E1B: return 0x0018F6; 
						case 0x000E19: return 0x0018EA; 
						case 0x000E23: return 0x001926; 
						case 0x000E28: return 0x001944; 
						case 0x000E09: return 0x00188A; 
						case 0x000E15: return 0x0018D2; 
						}

						*w = 1;
						return 0x001971;
					}
					else if (weight < state_000E40) {
						if (weight == state_001025) {
							switch (u) {
							case 0x00102E: return 0x001D4D; 
							}

							*w = 1;
							return 0x001D4C;
						}
						else if (weight < state_001025) {
							if (weight == state_001B0D) {
								switch (u) {
								case 0x001B35: return 0x001F3D; 
								}

								*w = 1;
								return 0x001F3C;
							}
						}
					}
					else { /* weight > state_000E40 */
						if (weight == state_000E41) {
							switch (u) {
							case 0x000E2C: return 0x00195D; 
							case 0x000E26: return 0x001939; 
							case 0x000E14: return 0x0018CD; 
							case 0x000E01: return 0x00185B; 
							case 0x000E0E: return 0x0018A9; 
							case 0x000E08: return 0x001885; 
							case 0x000E1D: return 0x001903; 
							case 0x000E11: return 0x0018BB; 
							case 0x000E0A: return 0x001891; 
							case 0x000E04: return 0x00186D; 
							case 0x000E29: return 0x00194B; 
							case 0x000E2B: return 0x001957; 
							case 0x000E25: return 0x001933; 
							case 0x000E15: return 0x0018D3; 
							case 0x000E1A: return 0x0018F1; 
							case 0x000E0D: return 0x0018A3; 
							case 0x000E21: return 0x00191B; 
							case 0x000E2E: return 0x001969; 
							case 0x000E1E: return 0x001909; 
							case 0x000E23: return 0x001927; 
							case 0x000E07: return 0x00187F; 
							case 0x000E2A: return 0x001951; 
							case 0x000E24: return 0x00192D; 
							case 0x000E12: return 0x0018C1; 
							case 0x000E03: return 0x001867; 
							case 0x000E20: return 0x001915; 
							case 0x000E16: return 0x0018D9; 
							case 0x000E28: return 0x001945; 
							case 0x000E2D: return 0x001963; 
							case 0x000E1B: return 0x0018F7; 
							case 0x000E0C: return 0x00189D; 
							case 0x000E06: return 0x001879; 
							case 0x000E1F: return 0x00190F; 
							case 0x000E13: return 0x0018C7; 
							case 0x000E18: return 0x0018E5; 
							case 0x000E02: return 0x001861; 
							case 0x000E0F: return 0x0018AF; 
							case 0x000E27: return 0x00193F; 
							case 0x000E17: return 0x0018DF; 
							case 0x000E1C: return 0x0018FD; 
							case 0x000E0B: return 0x001897; 
							case 0x000E09: return 0x00188B; 
							case 0x000E22: return 0x001921; 
							case 0x000E10: return 0x0018B5; 
							case 0x000E19: return 0x0018EB; 
							case 0x000E05: return 0x001873; 
							}

							*w = 1;
							return 0x001972;
						}
					}
				}
				else { /* weight > state_000E42 */
					if (weight == state_000E44) {
						switch (u) {
						case 0x000E1E: return 0x00190C; 
						case 0x000E10: return 0x0018B8; 
						case 0x000E08: return 0x001888; 
						case 0x000E01: return 0x00185E; 
						case 0x000E28: return 0x001948; 
						case 0x000E0E: return 0x0018AC; 
						case 0x000E14: return 0x0018D0; 
						case 0x000E2B: return 0x00195A; 
						case 0x000E27: return 0x001942; 
						case 0x000E29: return 0x00194E; 
						case 0x000E0A: return 0x001894; 
						case 0x000E18: return 0x0018E8; 
						case 0x000E22: return 0x001924; 
						case 0x000E1D: return 0x001906; 
						case 0x000E13: return 0x0018CA; 
						case 0x000E09: return 0x00188E; 
						case 0x000E06: return 0x00187C; 
						case 0x000E17: return 0x0018E2; 
						case 0x000E1C: return 0x001900; 
						case 0x000E2C: return 0x001960; 
						case 0x000E02: return 0x001864; 
						case 0x000E0F: return 0x0018B2; 
						case 0x000E12: return 0x0018C4; 
						case 0x000E24: return 0x001930; 
						case 0x000E0B: return 0x00189A; 
						case 0x000E07: return 0x001882; 
						case 0x000E16: return 0x0018DC; 
						case 0x000E20: return 0x001918; 
						case 0x000E23: return 0x00192A; 
						case 0x000E1B: return 0x0018FA; 
						case 0x000E2D: return 0x001966; 
						case 0x000E03: return 0x00186A; 
						case 0x000E1F: return 0x001912; 
						case 0x000E25: return 0x001936; 
						case 0x000E0C: return 0x0018A0; 
						case 0x000E04: return 0x001870; 
						case 0x000E11: return 0x0018BE; 
						case 0x000E21: return 0x00191E; 
						case 0x000E2E: return 0x00196C; 
						case 0x000E0D: return 0x0018A6; 
						case 0x000E15: return 0x0018D6; 
						case 0x000E1A: return 0x0018F4; 
						case 0x000E2A: return 0x001954; 
						case 0x000E26: return 0x00193C; 
						case 0x000E05: return 0x001876; 
						case 0x000E19: return 0x0018EE; 
						}

						*w = 1;
						return 0x001975;
					}
					else if (weight < state_000E44) {
						if (weight == state_000E43) {
							switch (u) {
							case 0x000E02: return 0x001863; 
							case 0x000E22: return 0x001923; 
							case 0x000E13: return 0x0018C9; 
							case 0x000E25: return 0x001935; 
							case 0x000E2A: return 0x001953; 
							case 0x000E06: return 0x00187B; 
							case 0x000E0B: return 0x001899; 
							case 0x000E28: return 0x001947; 
							case 0x000E1C: return 0x0018FF; 
							case 0x000E2E: return 0x00196B; 
							case 0x000E20: return 0x001917; 
							case 0x000E0F: return 0x0018B1; 
							case 0x000E01: return 0x00185D; 
							case 0x000E14: return 0x0018CF; 
							case 0x000E24: return 0x00192F; 
							case 0x000E05: return 0x001875; 
							case 0x000E0A: return 0x001893; 
							case 0x000E10: return 0x0018B7; 
							case 0x000E1D: return 0x001905; 
							case 0x000E2D: return 0x001965; 
							case 0x000E09: return 0x00188D; 
							case 0x000E0E: return 0x0018AB; 
							case 0x000E15: return 0x0018D5; 
							case 0x000E27: return 0x001941; 
							case 0x000E2C: return 0x00195F; 
							case 0x000E04: return 0x00186F; 
							case 0x000E18: return 0x0018E7; 
							case 0x000E11: return 0x0018BD; 
							case 0x000E1E: return 0x00190B; 
							case 0x000E08: return 0x001887; 
							case 0x000E23: return 0x001929; 
							case 0x000E0D: return 0x0018A5; 
							case 0x000E1A: return 0x0018F3; 
							case 0x000E16: return 0x0018DB; 
							case 0x000E26: return 0x00193B; 
							case 0x000E2B: return 0x001959; 
							case 0x000E03: return 0x001869; 
							case 0x000E19: return 0x0018ED; 
							case 0x000E29: return 0x00194D; 
							case 0x000E12: return 0x0018C3; 
							case 0x000E1F: return 0x001911; 
							case 0x000E07: return 0x001881; 
							case 0x000E0C: return 0x00189F; 
							case 0x000E1B: return 0x0018F9; 
							case 0x000E17: return 0x0018E1; 
							case 0x000E21: return 0x00191D; 
							}

							*w = 1;
							return 0x001974;
						}
					}
					else { /* weight > state_000E44 */
						if (weight == state_00AAB5) {
							switch (u) {
							case 0x00AAA8: return 0x001B2C; 
							case 0x00AA9F: return 0x001AF6; 
							case 0x00AA93: return 0x001AAE; 
							case 0x00AAAA: return 0x001B38; 
							case 0x00AAA7: return 0x001B26; 
							case 0x00AA82: return 0x001A48; 
							case 0x00AA8F: return 0x001A96; 
							case 0x00AA97: return 0x001AC6; 
							case 0x00AAAE: return 0x001B50; 
							case 0x00AA9C: return 0x001AE4; 
							case 0x00AAA0: return 0x001AFC; 
							case 0x00AA8B: return 0x001A7E; 
							case 0x00AAA9: return 0x001B32; 
							case 0x00AA90: return 0x001A9C; 
							case 0x00AAA4: return 0x001B14; 
							case 0x00AA85: return 0x001A5A; 
							case 0x00AA94: return 0x001AB4; 
							case 0x00AAAB: return 0x001B3E; 
							case 0x00AA81: return 0x001A42; 
							case 0x00AAA1: return 0x001B02; 
							case 0x00AA8E: return 0x001A90; 
							case 0x00AA98: return 0x001ACC; 
							case 0x00AAAF: return 0x001B56; 
							case 0x00AA9D: return 0x001AEA; 
							case 0x00AA91: return 0x001AA2; 
							case 0x00AAA5: return 0x001B1A; 
							case 0x00AA8A: return 0x001A78; 
							case 0x00AA84: return 0x001A54; 
							case 0x00AA95: return 0x001ABA; 
							case 0x00AA89: return 0x001A72; 
							case 0x00AAAC: return 0x001B44; 
							case 0x00AA9A: return 0x001AD8; 
							case 0x00AA80: return 0x001A3C; 
							case 0x00AA8D: return 0x001A8A; 
							case 0x00AA99: return 0x001AD2; 
							case 0x00AA9E: return 0x001AF0; 
							case 0x00AAA2: return 0x001B08; 
							case 0x00AA87: return 0x001A66; 
							case 0x00AA92: return 0x001AA8; 
							case 0x00AA88: return 0x001A6C; 
							case 0x00AAA6: return 0x001B20; 
							case 0x00AA83: return 0x001A4E; 
							case 0x00AA96: return 0x001AC0; 
							case 0x00AAAD: return 0x001B4A; 
							case 0x00AA9B: return 0x001ADE; 
							case 0x00AAA3: return 0x001B0E; 
							case 0x00AA8C: return 0x001A84; 
							case 0x00AA86: return 0x001A60; 
							}

							*w = 1;
							return 0x001B5C;
						}
					}
				}
			}
			else { /* weight > state_00AAB6 */
				if (weight == state_000EC2) {
					switch (u) {
					case 0x000E82: return 0x001986; 
					case 0x000E9B: return 0x0019DA; 
					case 0x000EDD: return 0x001A22; 
					case 0x000EAD: return 0x001A28; 
					case 0x000E9F: return 0x0019F2; 
					case 0x000EAA: return 0x00199E; 
					case 0x000E81: return 0x001980; 
					case 0x000E9C: return 0x0019E0; 
					case 0x000E94: return 0x0019B6; 
					case 0x000EDC: return 0x001A1C; 
					case 0x000EAE: return 0x001A2E; 
					case 0x000EA1: return 0x0019F8; 
					case 0x000E84: return 0x00198C; 
					case 0x000EA5: return 0x001A0A; 
					case 0x000EAB: return 0x001A16; 
					case 0x000E95: return 0x0019BC; 
					case 0x000EA2: return 0x0019FE; 
					case 0x000E99: return 0x0019CE; 
					case 0x000E8A: return 0x0019A4; 
					case 0x000EDF: return 0x0019AA; 
					case 0x000E88: return 0x001998; 
					case 0x000E9D: return 0x0019E6; 
					case 0x000E87: return 0x001992; 
					case 0x000E96: return 0x0019C2; 
					case 0x000E8D: return 0x0019B0; 
					case 0x000E9A: return 0x0019D4; 
					case 0x000EA3: return 0x001A04; 
					case 0x000EDE: return 0x00197A; 
					case 0x000E9E: return 0x0019EC; 
					case 0x000EA7: return 0x001A10; 
					case 0x000E97: return 0x0019C8; 
					}

					*w = 1;
					return 0x001A38;
				}
				else if (weight < state_000EC2) {
					if (weight == state_000EC0) {
						switch (u) {
						case 0x000E84: return 0x00198A; 
						case 0x000E9D: return 0x0019E4; 
						case 0x000E8A: return 0x0019A2; 
						case 0x000EAB: return 0x001A14; 
						case 0x000E88: return 0x001996; 
						case 0x000E99: return 0x0019CC; 
						case 0x000E96: return 0x0019C0; 
						case 0x000EA2: return 0x0019FC; 
						case 0x000E87: return 0x001990; 
						case 0x000E9E: return 0x0019EA; 
						case 0x000EDE: return 0x001978; 
						case 0x000E9A: return 0x0019D2; 
						case 0x000EA7: return 0x001A0E; 
						case 0x000E82: return 0x001984; 
						case 0x000E8D: return 0x0019AE; 
						case 0x000E97: return 0x0019C6; 
						case 0x000EA3: return 0x001A02; 
						case 0x000EAD: return 0x001A26; 
						case 0x000EDD: return 0x001A20; 
						case 0x000E9F: return 0x0019F0; 
						case 0x000E94: return 0x0019B4; 
						case 0x000E9B: return 0x0019D8; 
						case 0x000EAE: return 0x001A2C; 
						case 0x000E81: return 0x00197E; 
						case 0x000EAA: return 0x00199C; 
						case 0x000EA5: return 0x001A08; 
						case 0x000EDC: return 0x001A1A; 
						case 0x000E95: return 0x0019BA; 
						case 0x000E9C: return 0x0019DE; 
						case 0x000EA1: return 0x0019F6; 
						case 0x000EDF: return 0x0019A8; 
						}

						*w = 1;
						return 0x001A36;
					}
					else if (weight < state_000EC0) {
						if (weight == state_000EC1) {
							switch (u) {
							case 0x000EAD: return 0x001A27; 
							case 0x000E97: return 0x0019C7; 
							case 0x000EA1: return 0x0019F7; 
							case 0x000E9C: return 0x0019DF; 
							case 0x000E82: return 0x001985; 
							case 0x000EDE: return 0x001979; 
							case 0x000EA5: return 0x001A09; 
							case 0x000E87: return 0x001991; 
							case 0x000E96: return 0x0019C1; 
							case 0x000E9B: return 0x0019D9; 
							case 0x000E99: return 0x0019CD; 
							case 0x000E88: return 0x001997; 
							case 0x000EAB: return 0x001A15; 
							case 0x000E9F: return 0x0019F1; 
							case 0x000E84: return 0x00198B; 
							case 0x000EDF: return 0x0019A9; 
							case 0x000EA3: return 0x001A03; 
							case 0x000E8D: return 0x0019AF; 
							case 0x000E95: return 0x0019BB; 
							case 0x000EDC: return 0x001A1B; 
							case 0x000EA7: return 0x001A0F; 
							case 0x000E9A: return 0x0019D3; 
							case 0x000EAA: return 0x00199D; 
							case 0x000E9E: return 0x0019EB; 
							case 0x000EA2: return 0x0019FD; 
							case 0x000E81: return 0x00197F; 
							case 0x000EAE: return 0x001A2D; 
							case 0x000E94: return 0x0019B5; 
							case 0x000E8A: return 0x0019A3; 
							case 0x000EDD: return 0x001A21; 
							case 0x000E9D: return 0x0019E5; 
							}

							*w = 1;
							return 0x001A37;
						}
					}
					else { /* weight > state_000EC0 */
						if (weight == state_000EC3) {
							switch (u) {
							case 0x000E96: return 0x0019C3; 
							case 0x000EAE: return 0x001A2F; 
							case 0x000E9C: return 0x0019E1; 
							case 0x000EA1: return 0x0019F9; 
							case 0x000EDE: return 0x00197B; 
							case 0x000EA5: return 0x001A0B; 
							case 0x000E84: return 0x00198D; 
							case 0x000E95: return 0x0019BD; 
							case 0x000EAD: return 0x001A29; 
							case 0x000E9B: return 0x0019DB; 
							case 0x000E99: return 0x0019CF; 
							case 0x000EDF: return 0x0019AB; 
							case 0x000E9F: return 0x0019F3; 
							case 0x000E94: return 0x0019B7; 
							case 0x000E88: return 0x001999; 
							case 0x000EDC: return 0x001A1D; 
							case 0x000E81: return 0x001981; 
							case 0x000E9A: return 0x0019D5; 
							case 0x000E8D: return 0x0019B1; 
							case 0x000E9E: return 0x0019ED; 
							case 0x000EA3: return 0x001A05; 
							case 0x000EA7: return 0x001A11; 
							case 0x000EAB: return 0x001A17; 
							case 0x000E82: return 0x001987; 
							case 0x000E97: return 0x0019C9; 
							case 0x000E9D: return 0x0019E7; 
							case 0x000EA2: return 0x0019FF; 
							case 0x000E87: return 0x001993; 
							case 0x000E8A: return 0x0019A5; 
							case 0x000EDD: return 0x001A23; 
							case 0x000EAA: return 0x00199F; 
							}

							*w = 1;
							return 0x001A39;
						}
					}
				}
				else { /* weight > state_000EC2 */
					if (weight == state_001B0B) {
						switch (u) {
						case 0x001B35: return 0x001F3B; 
						}

						*w = 1;
						return 0x001F3A;
					}
					else if (weight < state_001B0B) {
						if (weight == state_000EC4) {
							switch (u) {
							case 0x000EDD: return 0x001A24; 
							case 0x000E9F: return 0x0019F4; 
							case 0x000EAA: return 0x0019A0; 
							case 0x000EA7: return 0x001A12; 
							case 0x000E82: return 0x001988; 
							case 0x000E97: return 0x0019CA; 
							case 0x000EDC: return 0x001A1E; 
							case 0x000EAE: return 0x001A30; 
							case 0x000E9C: return 0x0019E2; 
							case 0x000E94: return 0x0019B8; 
							case 0x000EAB: return 0x001A18; 
							case 0x000E81: return 0x001982; 
							case 0x000EA1: return 0x0019FA; 
							case 0x000EDF: return 0x0019AC; 
							case 0x000E9D: return 0x0019E8; 
							case 0x000EA5: return 0x001A0C; 
							case 0x000E8A: return 0x0019A6; 
							case 0x000E84: return 0x00198E; 
							case 0x000E95: return 0x0019BE; 
							case 0x000E9A: return 0x0019D6; 
							case 0x000E8D: return 0x0019B2; 
							case 0x000E99: return 0x0019D0; 
							case 0x000E9E: return 0x0019EE; 
							case 0x000EA2: return 0x001A00; 
							case 0x000E87: return 0x001994; 
							case 0x000EDE: return 0x00197C; 
							case 0x000E88: return 0x00199A; 
							case 0x000E96: return 0x0019C4; 
							case 0x000EAD: return 0x001A2A; 
							case 0x000E9B: return 0x0019DC; 
							case 0x000EA3: return 0x001A06; 
							}

							*w = 1;
							return 0x001A3A;
						}
					}
					else { /* weight > state_001B0B */
						if (weight == state_001B11) {
							switch (u) {
							case 0x001B35: return 0x001F41; 
							}

							*w = 1;
							return 0x001F40;
						}
					}
				}
			}
		}
	}

	return 0;
}
