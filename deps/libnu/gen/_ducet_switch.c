/* Automatically generated file (contractions-toc), 1779042524
 *
 * Tag          : _nu_ducet
 * Contractions : 901
 */

#include <stdint.h>

#include "../udb.h"

const size_t _NU_DUCET_CONTRACTIONS = 901; /* contractions included in switch */
const size_t _NU_DUCET_CODEPOINTS = 27985; /* complementary codepoints number */

#define state_016D69 -943
#define state_016D67 -941
#define state_016D63_016D67 -938
#define state_016D63 -937
#define state_011390 -935
#define state_01138B -933
#define state_011384 -931
#define state_011382 -929
#define state_0105DA -927
#define state_0105D2 -925
#define state_00AABC -876
#define state_00AABB -827
#define state_00AAB9 -778
#define state_00AAB6 -729
#define state_00AAB5 -680
#define state_001B11 -678
#define state_001B0D -676
#define state_001B0B -674
#define state_001B09 -672
#define state_001B07 -670
#define state_001B05 -668
#define state_0019BA -623
#define state_0019B7 -578
#define state_0019B6 -533
#define state_0019B5 -488
#define state_001025 -486
#define state_000EC4 -440
#define state_000EC3 -394
#define state_000EC2 -348
#define state_000EC1 -302
#define state_000EC0 -256
#define state_000E44 -209
#define state_000E43 -162
#define state_000E42 -115
#define state_000E41 -68
#define state_000E40 -21
#define state_000B92 -19
#define state_00064A -17
#define state_000648 -15
#define state_000627 -11
#define state_000438 -9
#define state_000418 -7
#define state_00006C -4
#define state_00004C -1

const int16_t _NU_DUCET_ROOTS_G[] = {
	2, 7, 9, 0, -38, -37, 1, 0, -34, 0, -33, -32, 
	-31, 0, 0, 0, -30, 0, -29, 0, 1, 0, 0, 10, 
	1, 13, -28, 3, -27, -25, 1, -23, -16, 0, -13, -9, 
	-3, 0, 0, 0, 0, 1, 25, };

const size_t _NU_DUCET_ROOTS_G_SIZE = sizeof(_NU_DUCET_ROOTS_G) / sizeof(*_NU_DUCET_ROOTS_G);

/* codepoints */
const uint32_t _NU_DUCET_ROOTS_VALUES_C[] = {
	0x000E40, 0x000EC0, 0x00AAB9, 0x001B07, 0x0019B6, 0x0019B5, 0x000438, 0x001B11, 
	0x000418, 0x00064A, 0x0019B7, 0x011382, 0x00AABB, 0x016D63, 0x000648, 0x00AAB5, 
	0x01138B, 0x016D67, 0x000EC3, 0x016D69, 0x000E42, 0x00AABC, 0x00AAB6, 0x00006C, 
	0x000627, 0x011390, 0x0019BA, 0x011384, 0x0105DA, 0x000B92, 0x001B0D, 0x001025, 
	0x0105D2, 0x001B09, 0x00004C, 0x000EC2, 0x000E44, 0x001B05, 0x000E43, 0x000E41, 
	0x000EC4, 0x000EC1, 0x001B0B, };

/* indexes */
const uint16_t _NU_DUCET_ROOTS_VALUES_I[] = {
	0x0015, 0x0100, 0x030A, 0x029E, 0x0215, 0x01E8, 0x0009, 0x02A6, 0x0007, 0x0011, 
	0x0242, 0x03A1, 0x033B, 0x03A9, 0x000F, 0x02A8, 0x03A5, 0x03AD, 0x018A, 0x03AF, 
	0x0073, 0x036C, 0x02D9, 0x0004, 0x000B, 0x03A7, 0x026F, 0x03A3, 0x039F, 0x0013, 
	0x02A4, 0x01E6, 0x039D, 0x02A0, 0x0001, 0x015C, 0x00D1, 0x029C, 0x00A2, 0x0044, 
	0x01B8, 0x012E, 0x02A2, };

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

		if (weight == state_0019B7) {
			switch (u) {
			case 0x001980: return 0x0028CE; 
			case 0x001981: return 0x0028D3; 
			case 0x001982: return 0x0028D8; 
			case 0x001983: return 0x0028DD; 
			case 0x001984: return 0x0028E2; 
			case 0x001985: return 0x0028E7; 
			case 0x001986: return 0x0028EC; 
			case 0x001987: return 0x0028F1; 
			case 0x001988: return 0x0028F6; 
			case 0x001989: return 0x0028FB; 
			case 0x00198A: return 0x002900; 
			case 0x00198B: return 0x002905; 
			case 0x00198C: return 0x00290A; 
			case 0x00198D: return 0x00290F; 
			case 0x00198E: return 0x002914; 
			case 0x00198F: return 0x002919; 
			case 0x001990: return 0x00291E; 
			case 0x001991: return 0x002923; 
			case 0x001992: return 0x002928; 
			case 0x001993: return 0x00292D; 
			case 0x001994: return 0x002932; 
			case 0x001995: return 0x002937; 
			case 0x001996: return 0x00293C; 
			case 0x001997: return 0x002941; 
			case 0x001998: return 0x002946; 
			case 0x001999: return 0x00294B; 
			case 0x00199A: return 0x002950; 
			case 0x00199B: return 0x002955; 
			case 0x00199C: return 0x00295A; 
			case 0x00199D: return 0x00295F; 
			case 0x00199E: return 0x002964; 
			case 0x00199F: return 0x002969; 
			case 0x0019A0: return 0x00296E; 
			case 0x0019A1: return 0x002973; 
			case 0x0019A2: return 0x002978; 
			case 0x0019A3: return 0x00297D; 
			case 0x0019A4: return 0x002982; 
			case 0x0019A5: return 0x002987; 
			case 0x0019A6: return 0x00298C; 
			case 0x0019A7: return 0x002991; 
			case 0x0019A8: return 0x002996; 
			case 0x0019A9: return 0x00299B; 
			case 0x0019AA: return 0x0029A0; 
			case 0x0019AB: return 0x0029A5; 
			}

			*w = 1;
			return 0x0029AE;
		}
		else if (weight < state_0019B7) {
			if (weight == state_00AABB) {
				switch (u) {
				case 0x00AA80: return 0x0024A0; 
				case 0x00AA81: return 0x0024A6; 
				case 0x00AA82: return 0x0024AC; 
				case 0x00AA83: return 0x0024B2; 
				case 0x00AA84: return 0x0024B8; 
				case 0x00AA85: return 0x0024BE; 
				case 0x00AA86: return 0x0024C4; 
				case 0x00AA87: return 0x0024CA; 
				case 0x00AA88: return 0x0024D0; 
				case 0x00AA89: return 0x0024D6; 
				case 0x00AA8A: return 0x0024DC; 
				case 0x00AA8B: return 0x0024E2; 
				case 0x00AA8C: return 0x0024E8; 
				case 0x00AA8D: return 0x0024EE; 
				case 0x00AA8E: return 0x0024F4; 
				case 0x00AA8F: return 0x0024FA; 
				case 0x00AA90: return 0x002500; 
				case 0x00AA91: return 0x002506; 
				case 0x00AA92: return 0x00250C; 
				case 0x00AA93: return 0x002512; 
				case 0x00AA94: return 0x002518; 
				case 0x00AA95: return 0x00251E; 
				case 0x00AA96: return 0x002524; 
				case 0x00AA97: return 0x00252A; 
				case 0x00AA98: return 0x002530; 
				case 0x00AA99: return 0x002536; 
				case 0x00AA9A: return 0x00253C; 
				case 0x00AA9B: return 0x002542; 
				case 0x00AA9C: return 0x002548; 
				case 0x00AA9D: return 0x00254E; 
				case 0x00AA9E: return 0x002554; 
				case 0x00AA9F: return 0x00255A; 
				case 0x00AAA0: return 0x002560; 
				case 0x00AAA1: return 0x002566; 
				case 0x00AAA2: return 0x00256C; 
				case 0x00AAA3: return 0x002572; 
				case 0x00AAA4: return 0x002578; 
				case 0x00AAA5: return 0x00257E; 
				case 0x00AAA6: return 0x002584; 
				case 0x00AAA7: return 0x00258A; 
				case 0x00AAA8: return 0x002590; 
				case 0x00AAA9: return 0x002596; 
				case 0x00AAAA: return 0x00259C; 
				case 0x00AAAB: return 0x0025A2; 
				case 0x00AAAC: return 0x0025A8; 
				case 0x00AAAD: return 0x0025AE; 
				case 0x00AAAE: return 0x0025B4; 
				case 0x00AAAF: return 0x0025BA; 
				}

				*w = 1;
				return 0x0025C1;
			}
			else if (weight < state_00AABB) {
				if (weight == state_01138B) {
					switch (u) {
					case 0x0113C2: return 0x001F5C; 
					}

					*w = 1;
					return 0x001F5B;
				}
				else if (weight < state_01138B) {
					if (weight == state_016D63_016D67) {
						switch (u) {
						case 0x016D67: return 0x004480; 
						}

						*w = 1;
						return 0x00447F;
					}
					else if (weight < state_016D63_016D67) {
						if (weight == state_016D67) {
							switch (u) {
							case 0x016D67: return 0x00447E; 
							}

							*w = 1;
							return 0x00447D;
						}
						else if (weight < state_016D67) {
							if (weight == state_016D69) {
								switch (u) {
								case 0x016D67: return 0x004480; 
								}

								*w = 1;
								return 0x00447F;
							}
						}
					}
					else { /* weight > state_016D63_016D67 */
						if (weight == state_011390) {
							switch (u) {
							case 0x0113C9: return 0x001F5E; 
							}

							*w = 1;
							return 0x001F5D;
						}
						else if (weight < state_011390) {
							if (weight == state_016D63) {
								switch (u) {
								case 0x016D67: return state_016D63_016D67; 
								case 0x016D68: return 0x004480; 
								}

								*w = 1;
								return 0x004479;
							}
						}
					}
				}
				else { /* weight > state_01138B */
					if (weight == state_0105DA) {
						switch (u) {
						case 0x000307: return 0x004383; 
						}

						*w = 1;
						return 0x004379;
					}
					else if (weight < state_0105DA) {
						if (weight == state_011382) {
							switch (u) {
							case 0x0113C9: return 0x001F54; 
							}

							*w = 1;
							return 0x001F53;
						}
						else if (weight < state_011382) {
							if (weight == state_011384) {
								switch (u) {
								case 0x0113BB: return 0x001F56; 
								}

								*w = 1;
								return 0x001F55;
							}
						}
					}
					else { /* weight > state_0105DA */
						if (weight == state_00AABC) {
							switch (u) {
							case 0x00AA80: return 0x0024A1; 
							case 0x00AA81: return 0x0024A7; 
							case 0x00AA82: return 0x0024AD; 
							case 0x00AA83: return 0x0024B3; 
							case 0x00AA84: return 0x0024B9; 
							case 0x00AA85: return 0x0024BF; 
							case 0x00AA86: return 0x0024C5; 
							case 0x00AA87: return 0x0024CB; 
							case 0x00AA88: return 0x0024D1; 
							case 0x00AA89: return 0x0024D7; 
							case 0x00AA8A: return 0x0024DD; 
							case 0x00AA8B: return 0x0024E3; 
							case 0x00AA8C: return 0x0024E9; 
							case 0x00AA8D: return 0x0024EF; 
							case 0x00AA8E: return 0x0024F5; 
							case 0x00AA8F: return 0x0024FB; 
							case 0x00AA90: return 0x002501; 
							case 0x00AA91: return 0x002507; 
							case 0x00AA92: return 0x00250D; 
							case 0x00AA93: return 0x002513; 
							case 0x00AA94: return 0x002519; 
							case 0x00AA95: return 0x00251F; 
							case 0x00AA96: return 0x002525; 
							case 0x00AA97: return 0x00252B; 
							case 0x00AA98: return 0x002531; 
							case 0x00AA99: return 0x002537; 
							case 0x00AA9A: return 0x00253D; 
							case 0x00AA9B: return 0x002543; 
							case 0x00AA9C: return 0x002549; 
							case 0x00AA9D: return 0x00254F; 
							case 0x00AA9E: return 0x002555; 
							case 0x00AA9F: return 0x00255B; 
							case 0x00AAA0: return 0x002561; 
							case 0x00AAA1: return 0x002567; 
							case 0x00AAA2: return 0x00256D; 
							case 0x00AAA3: return 0x002573; 
							case 0x00AAA4: return 0x002579; 
							case 0x00AAA5: return 0x00257F; 
							case 0x00AAA6: return 0x002585; 
							case 0x00AAA7: return 0x00258B; 
							case 0x00AAA8: return 0x002591; 
							case 0x00AAA9: return 0x002597; 
							case 0x00AAAA: return 0x00259D; 
							case 0x00AAAB: return 0x0025A3; 
							case 0x00AAAC: return 0x0025A9; 
							case 0x00AAAD: return 0x0025AF; 
							case 0x00AAAE: return 0x0025B5; 
							case 0x00AAAF: return 0x0025BB; 
							}

							*w = 1;
							return 0x0025C2;
						}
						else if (weight < state_00AABC) {
							if (weight == state_0105D2) {
								switch (u) {
								case 0x000307: return 0x004368; 
								}

								*w = 1;
								return 0x004371;
							}
						}
					}
				}
			}
			else { /* weight > state_00AABB */
				if (weight == state_001B0B) {
					switch (u) {
					case 0x001B35: return 0x002A62; 
					}

					*w = 1;
					return 0x002A61;
				}
				else if (weight < state_001B0B) {
					if (weight == state_00AAB5) {
						switch (u) {
						case 0x00AA80: return 0x00249D; 
						case 0x00AA81: return 0x0024A3; 
						case 0x00AA82: return 0x0024A9; 
						case 0x00AA83: return 0x0024AF; 
						case 0x00AA84: return 0x0024B5; 
						case 0x00AA85: return 0x0024BB; 
						case 0x00AA86: return 0x0024C1; 
						case 0x00AA87: return 0x0024C7; 
						case 0x00AA88: return 0x0024CD; 
						case 0x00AA89: return 0x0024D3; 
						case 0x00AA8A: return 0x0024D9; 
						case 0x00AA8B: return 0x0024DF; 
						case 0x00AA8C: return 0x0024E5; 
						case 0x00AA8D: return 0x0024EB; 
						case 0x00AA8E: return 0x0024F1; 
						case 0x00AA8F: return 0x0024F7; 
						case 0x00AA90: return 0x0024FD; 
						case 0x00AA91: return 0x002503; 
						case 0x00AA92: return 0x002509; 
						case 0x00AA93: return 0x00250F; 
						case 0x00AA94: return 0x002515; 
						case 0x00AA95: return 0x00251B; 
						case 0x00AA96: return 0x002521; 
						case 0x00AA97: return 0x002527; 
						case 0x00AA98: return 0x00252D; 
						case 0x00AA99: return 0x002533; 
						case 0x00AA9A: return 0x002539; 
						case 0x00AA9B: return 0x00253F; 
						case 0x00AA9C: return 0x002545; 
						case 0x00AA9D: return 0x00254B; 
						case 0x00AA9E: return 0x002551; 
						case 0x00AA9F: return 0x002557; 
						case 0x00AAA0: return 0x00255D; 
						case 0x00AAA1: return 0x002563; 
						case 0x00AAA2: return 0x002569; 
						case 0x00AAA3: return 0x00256F; 
						case 0x00AAA4: return 0x002575; 
						case 0x00AAA5: return 0x00257B; 
						case 0x00AAA6: return 0x002581; 
						case 0x00AAA7: return 0x002587; 
						case 0x00AAA8: return 0x00258D; 
						case 0x00AAA9: return 0x002593; 
						case 0x00AAAA: return 0x002599; 
						case 0x00AAAB: return 0x00259F; 
						case 0x00AAAC: return 0x0025A5; 
						case 0x00AAAD: return 0x0025AB; 
						case 0x00AAAE: return 0x0025B1; 
						case 0x00AAAF: return 0x0025B7; 
						}

						*w = 1;
						return 0x0025BD;
					}
					else if (weight < state_00AAB5) {
						if (weight == state_00AAB6) {
							switch (u) {
							case 0x00AA80: return 0x00249E; 
							case 0x00AA81: return 0x0024A4; 
							case 0x00AA82: return 0x0024AA; 
							case 0x00AA83: return 0x0024B0; 
							case 0x00AA84: return 0x0024B6; 
							case 0x00AA85: return 0x0024BC; 
							case 0x00AA86: return 0x0024C2; 
							case 0x00AA87: return 0x0024C8; 
							case 0x00AA88: return 0x0024CE; 
							case 0x00AA89: return 0x0024D4; 
							case 0x00AA8A: return 0x0024DA; 
							case 0x00AA8B: return 0x0024E0; 
							case 0x00AA8C: return 0x0024E6; 
							case 0x00AA8D: return 0x0024EC; 
							case 0x00AA8E: return 0x0024F2; 
							case 0x00AA8F: return 0x0024F8; 
							case 0x00AA90: return 0x0024FE; 
							case 0x00AA91: return 0x002504; 
							case 0x00AA92: return 0x00250A; 
							case 0x00AA93: return 0x002510; 
							case 0x00AA94: return 0x002516; 
							case 0x00AA95: return 0x00251C; 
							case 0x00AA96: return 0x002522; 
							case 0x00AA97: return 0x002528; 
							case 0x00AA98: return 0x00252E; 
							case 0x00AA99: return 0x002534; 
							case 0x00AA9A: return 0x00253A; 
							case 0x00AA9B: return 0x002540; 
							case 0x00AA9C: return 0x002546; 
							case 0x00AA9D: return 0x00254C; 
							case 0x00AA9E: return 0x002552; 
							case 0x00AA9F: return 0x002558; 
							case 0x00AAA0: return 0x00255E; 
							case 0x00AAA1: return 0x002564; 
							case 0x00AAA2: return 0x00256A; 
							case 0x00AAA3: return 0x002570; 
							case 0x00AAA4: return 0x002576; 
							case 0x00AAA5: return 0x00257C; 
							case 0x00AAA6: return 0x002582; 
							case 0x00AAA7: return 0x002588; 
							case 0x00AAA8: return 0x00258E; 
							case 0x00AAA9: return 0x002594; 
							case 0x00AAAA: return 0x00259A; 
							case 0x00AAAB: return 0x0025A0; 
							case 0x00AAAC: return 0x0025A6; 
							case 0x00AAAD: return 0x0025AC; 
							case 0x00AAAE: return 0x0025B2; 
							case 0x00AAAF: return 0x0025B8; 
							}

							*w = 1;
							return 0x0025BE;
						}
						else if (weight < state_00AAB6) {
							if (weight == state_00AAB9) {
								switch (u) {
								case 0x00AA80: return 0x00249F; 
								case 0x00AA81: return 0x0024A5; 
								case 0x00AA82: return 0x0024AB; 
								case 0x00AA83: return 0x0024B1; 
								case 0x00AA84: return 0x0024B7; 
								case 0x00AA85: return 0x0024BD; 
								case 0x00AA86: return 0x0024C3; 
								case 0x00AA87: return 0x0024C9; 
								case 0x00AA88: return 0x0024CF; 
								case 0x00AA89: return 0x0024D5; 
								case 0x00AA8A: return 0x0024DB; 
								case 0x00AA8B: return 0x0024E1; 
								case 0x00AA8C: return 0x0024E7; 
								case 0x00AA8D: return 0x0024ED; 
								case 0x00AA8E: return 0x0024F3; 
								case 0x00AA8F: return 0x0024F9; 
								case 0x00AA90: return 0x0024FF; 
								case 0x00AA91: return 0x002505; 
								case 0x00AA92: return 0x00250B; 
								case 0x00AA93: return 0x002511; 
								case 0x00AA94: return 0x002517; 
								case 0x00AA95: return 0x00251D; 
								case 0x00AA96: return 0x002523; 
								case 0x00AA97: return 0x002529; 
								case 0x00AA98: return 0x00252F; 
								case 0x00AA99: return 0x002535; 
								case 0x00AA9A: return 0x00253B; 
								case 0x00AA9B: return 0x002541; 
								case 0x00AA9C: return 0x002547; 
								case 0x00AA9D: return 0x00254D; 
								case 0x00AA9E: return 0x002553; 
								case 0x00AA9F: return 0x002559; 
								case 0x00AAA0: return 0x00255F; 
								case 0x00AAA1: return 0x002565; 
								case 0x00AAA2: return 0x00256B; 
								case 0x00AAA3: return 0x002571; 
								case 0x00AAA4: return 0x002577; 
								case 0x00AAA5: return 0x00257D; 
								case 0x00AAA6: return 0x002583; 
								case 0x00AAA7: return 0x002589; 
								case 0x00AAA8: return 0x00258F; 
								case 0x00AAA9: return 0x002595; 
								case 0x00AAAA: return 0x00259B; 
								case 0x00AAAB: return 0x0025A1; 
								case 0x00AAAC: return 0x0025A7; 
								case 0x00AAAD: return 0x0025AD; 
								case 0x00AAAE: return 0x0025B3; 
								case 0x00AAAF: return 0x0025B9; 
								}

								*w = 1;
								return 0x0025BF;
							}
						}
					}
					else { /* weight > state_00AAB5 */
						if (weight == state_001B0D) {
							switch (u) {
							case 0x001B35: return 0x002A64; 
							}

							*w = 1;
							return 0x002A63;
						}
						else if (weight < state_001B0D) {
							if (weight == state_001B11) {
								switch (u) {
								case 0x001B35: return 0x002A68; 
								}

								*w = 1;
								return 0x002A67;
							}
						}
					}
				}
				else { /* weight > state_001B0B */
					if (weight == state_001B05) {
						switch (u) {
						case 0x001B35: return 0x002A5C; 
						}

						*w = 1;
						return 0x002A5B;
					}
					else if (weight < state_001B05) {
						if (weight == state_001B07) {
							switch (u) {
							case 0x001B35: return 0x002A5E; 
							}

							*w = 1;
							return 0x002A5D;
						}
						else if (weight < state_001B07) {
							if (weight == state_001B09) {
								switch (u) {
								case 0x001B35: return 0x002A60; 
								}

								*w = 1;
								return 0x002A5F;
							}
						}
					}
					else { /* weight > state_001B05 */
						if (weight == state_0019BA) {
							switch (u) {
							case 0x001980: return 0x0028CF; 
							case 0x001981: return 0x0028D4; 
							case 0x001982: return 0x0028D9; 
							case 0x001983: return 0x0028DE; 
							case 0x001984: return 0x0028E3; 
							case 0x001985: return 0x0028E8; 
							case 0x001986: return 0x0028ED; 
							case 0x001987: return 0x0028F2; 
							case 0x001988: return 0x0028F7; 
							case 0x001989: return 0x0028FC; 
							case 0x00198A: return 0x002901; 
							case 0x00198B: return 0x002906; 
							case 0x00198C: return 0x00290B; 
							case 0x00198D: return 0x002910; 
							case 0x00198E: return 0x002915; 
							case 0x00198F: return 0x00291A; 
							case 0x001990: return 0x00291F; 
							case 0x001991: return 0x002924; 
							case 0x001992: return 0x002929; 
							case 0x001993: return 0x00292E; 
							case 0x001994: return 0x002933; 
							case 0x001995: return 0x002938; 
							case 0x001996: return 0x00293D; 
							case 0x001997: return 0x002942; 
							case 0x001998: return 0x002947; 
							case 0x001999: return 0x00294C; 
							case 0x00199A: return 0x002951; 
							case 0x00199B: return 0x002956; 
							case 0x00199C: return 0x00295B; 
							case 0x00199D: return 0x002960; 
							case 0x00199E: return 0x002965; 
							case 0x00199F: return 0x00296A; 
							case 0x0019A0: return 0x00296F; 
							case 0x0019A1: return 0x002974; 
							case 0x0019A2: return 0x002979; 
							case 0x0019A3: return 0x00297E; 
							case 0x0019A4: return 0x002983; 
							case 0x0019A5: return 0x002988; 
							case 0x0019A6: return 0x00298D; 
							case 0x0019A7: return 0x002992; 
							case 0x0019A8: return 0x002997; 
							case 0x0019A9: return 0x00299C; 
							case 0x0019AA: return 0x0029A1; 
							case 0x0019AB: return 0x0029A6; 
							}

							*w = 1;
							return 0x0029B1;
						}
					}
				}
			}
		}
		else { /* weight > state_0019B7 */
			if (weight == state_000E42) {
				switch (u) {
				case 0x000E01: return 0x002269; 
				case 0x000E02: return 0x00226F; 
				case 0x000E03: return 0x002275; 
				case 0x000E04: return 0x00227B; 
				case 0x000E05: return 0x002281; 
				case 0x000E06: return 0x002287; 
				case 0x000E07: return 0x00228D; 
				case 0x000E08: return 0x002293; 
				case 0x000E09: return 0x002299; 
				case 0x000E0A: return 0x00229F; 
				case 0x000E0B: return 0x0022A5; 
				case 0x000E0C: return 0x0022AB; 
				case 0x000E0D: return 0x0022B1; 
				case 0x000E0E: return 0x0022B7; 
				case 0x000E0F: return 0x0022BD; 
				case 0x000E10: return 0x0022C3; 
				case 0x000E11: return 0x0022C9; 
				case 0x000E12: return 0x0022CF; 
				case 0x000E13: return 0x0022D5; 
				case 0x000E14: return 0x0022DB; 
				case 0x000E15: return 0x0022E1; 
				case 0x000E16: return 0x0022E7; 
				case 0x000E17: return 0x0022ED; 
				case 0x000E18: return 0x0022F3; 
				case 0x000E19: return 0x0022F9; 
				case 0x000E1A: return 0x0022FF; 
				case 0x000E1B: return 0x002305; 
				case 0x000E1C: return 0x00230B; 
				case 0x000E1D: return 0x002311; 
				case 0x000E1E: return 0x002317; 
				case 0x000E1F: return 0x00231D; 
				case 0x000E20: return 0x002323; 
				case 0x000E21: return 0x002329; 
				case 0x000E22: return 0x00232F; 
				case 0x000E23: return 0x002335; 
				case 0x000E24: return 0x00233B; 
				case 0x000E25: return 0x002341; 
				case 0x000E26: return 0x002347; 
				case 0x000E27: return 0x00234D; 
				case 0x000E28: return 0x002353; 
				case 0x000E29: return 0x002359; 
				case 0x000E2A: return 0x00235F; 
				case 0x000E2B: return 0x002365; 
				case 0x000E2C: return 0x00236B; 
				case 0x000E2D: return 0x002371; 
				case 0x000E2E: return 0x002377; 
				}

				*w = 1;
				return 0x002380;
			}
			else if (weight < state_000E42) {
				if (weight == state_000EC2) {
					switch (u) {
					case 0x000E81: return 0x00238D; 
					case 0x000E82: return 0x002393; 
					case 0x000E84: return 0x002399; 
					case 0x000E86: return 0x00239F; 
					case 0x000E87: return 0x0023A5; 
					case 0x000E88: return 0x0023AB; 
					case 0x000E89: return 0x0023B1; 
					case 0x000E8A: return 0x0023BD; 
					case 0x000E8C: return 0x0023C3; 
					case 0x000E8D: return 0x0023D5; 
					case 0x000E8E: return 0x0023C9; 
					case 0x000E8F: return 0x0023DB; 
					case 0x000E90: return 0x0023E1; 
					case 0x000E91: return 0x0023E7; 
					case 0x000E92: return 0x0023ED; 
					case 0x000E93: return 0x0023F3; 
					case 0x000E94: return 0x0023F9; 
					case 0x000E95: return 0x0023FF; 
					case 0x000E96: return 0x002405; 
					case 0x000E97: return 0x00240B; 
					case 0x000E98: return 0x002411; 
					case 0x000E99: return 0x002417; 
					case 0x000E9A: return 0x00241D; 
					case 0x000E9B: return 0x002423; 
					case 0x000E9C: return 0x002429; 
					case 0x000E9D: return 0x00242F; 
					case 0x000E9E: return 0x002435; 
					case 0x000E9F: return 0x00243B; 
					case 0x000EA0: return 0x002441; 
					case 0x000EA1: return 0x002447; 
					case 0x000EA2: return 0x00244D; 
					case 0x000EA3: return 0x002453; 
					case 0x000EA5: return 0x002459; 
					case 0x000EA7: return 0x00245F; 
					case 0x000EA8: return 0x002465; 
					case 0x000EA9: return 0x00246B; 
					case 0x000EAA: return 0x0023B7; 
					case 0x000EAB: return 0x002471; 
					case 0x000EAC: return 0x002483; 
					case 0x000EAD: return 0x002489; 
					case 0x000EAE: return 0x00248F; 
					case 0x000EDC: return 0x002477; 
					case 0x000EDD: return 0x00247D; 
					case 0x000EDE: return 0x002387; 
					case 0x000EDF: return 0x0023CF; 
					}

					*w = 1;
					return 0x002499;
				}
				else if (weight < state_000EC2) {
					if (weight == state_001025) {
						switch (u) {
						case 0x00102E: return 0x00281D; 
						}

						*w = 1;
						return 0x00281C;
					}
					else if (weight < state_001025) {
						if (weight == state_0019B5) {
							switch (u) {
							case 0x001980: return 0x0028CC; 
							case 0x001981: return 0x0028D1; 
							case 0x001982: return 0x0028D6; 
							case 0x001983: return 0x0028DB; 
							case 0x001984: return 0x0028E0; 
							case 0x001985: return 0x0028E5; 
							case 0x001986: return 0x0028EA; 
							case 0x001987: return 0x0028EF; 
							case 0x001988: return 0x0028F4; 
							case 0x001989: return 0x0028F9; 
							case 0x00198A: return 0x0028FE; 
							case 0x00198B: return 0x002903; 
							case 0x00198C: return 0x002908; 
							case 0x00198D: return 0x00290D; 
							case 0x00198E: return 0x002912; 
							case 0x00198F: return 0x002917; 
							case 0x001990: return 0x00291C; 
							case 0x001991: return 0x002921; 
							case 0x001992: return 0x002926; 
							case 0x001993: return 0x00292B; 
							case 0x001994: return 0x002930; 
							case 0x001995: return 0x002935; 
							case 0x001996: return 0x00293A; 
							case 0x001997: return 0x00293F; 
							case 0x001998: return 0x002944; 
							case 0x001999: return 0x002949; 
							case 0x00199A: return 0x00294E; 
							case 0x00199B: return 0x002953; 
							case 0x00199C: return 0x002958; 
							case 0x00199D: return 0x00295D; 
							case 0x00199E: return 0x002962; 
							case 0x00199F: return 0x002967; 
							case 0x0019A0: return 0x00296C; 
							case 0x0019A1: return 0x002971; 
							case 0x0019A2: return 0x002976; 
							case 0x0019A3: return 0x00297B; 
							case 0x0019A4: return 0x002980; 
							case 0x0019A5: return 0x002985; 
							case 0x0019A6: return 0x00298A; 
							case 0x0019A7: return 0x00298F; 
							case 0x0019A8: return 0x002994; 
							case 0x0019A9: return 0x002999; 
							case 0x0019AA: return 0x00299E; 
							case 0x0019AB: return 0x0029A3; 
							}

							*w = 1;
							return 0x0029AC;
						}
						else if (weight < state_0019B5) {
							if (weight == state_0019B6) {
								switch (u) {
								case 0x001980: return 0x0028CD; 
								case 0x001981: return 0x0028D2; 
								case 0x001982: return 0x0028D7; 
								case 0x001983: return 0x0028DC; 
								case 0x001984: return 0x0028E1; 
								case 0x001985: return 0x0028E6; 
								case 0x001986: return 0x0028EB; 
								case 0x001987: return 0x0028F0; 
								case 0x001988: return 0x0028F5; 
								case 0x001989: return 0x0028FA; 
								case 0x00198A: return 0x0028FF; 
								case 0x00198B: return 0x002904; 
								case 0x00198C: return 0x002909; 
								case 0x00198D: return 0x00290E; 
								case 0x00198E: return 0x002913; 
								case 0x00198F: return 0x002918; 
								case 0x001990: return 0x00291D; 
								case 0x001991: return 0x002922; 
								case 0x001992: return 0x002927; 
								case 0x001993: return 0x00292C; 
								case 0x001994: return 0x002931; 
								case 0x001995: return 0x002936; 
								case 0x001996: return 0x00293B; 
								case 0x001997: return 0x002940; 
								case 0x001998: return 0x002945; 
								case 0x001999: return 0x00294A; 
								case 0x00199A: return 0x00294F; 
								case 0x00199B: return 0x002954; 
								case 0x00199C: return 0x002959; 
								case 0x00199D: return 0x00295E; 
								case 0x00199E: return 0x002963; 
								case 0x00199F: return 0x002968; 
								case 0x0019A0: return 0x00296D; 
								case 0x0019A1: return 0x002972; 
								case 0x0019A2: return 0x002977; 
								case 0x0019A3: return 0x00297C; 
								case 0x0019A4: return 0x002981; 
								case 0x0019A5: return 0x002986; 
								case 0x0019A6: return 0x00298B; 
								case 0x0019A7: return 0x002990; 
								case 0x0019A8: return 0x002995; 
								case 0x0019A9: return 0x00299A; 
								case 0x0019AA: return 0x00299F; 
								case 0x0019AB: return 0x0029A4; 
								}

								*w = 1;
								return 0x0029AD;
							}
						}
					}
					else { /* weight > state_001025 */
						if (weight == state_000EC3) {
							switch (u) {
							case 0x000E81: return 0x00238E; 
							case 0x000E82: return 0x002394; 
							case 0x000E84: return 0x00239A; 
							case 0x000E86: return 0x0023A0; 
							case 0x000E87: return 0x0023A6; 
							case 0x000E88: return 0x0023AC; 
							case 0x000E89: return 0x0023B2; 
							case 0x000E8A: return 0x0023BE; 
							case 0x000E8C: return 0x0023C4; 
							case 0x000E8D: return 0x0023D6; 
							case 0x000E8E: return 0x0023CA; 
							case 0x000E8F: return 0x0023DC; 
							case 0x000E90: return 0x0023E2; 
							case 0x000E91: return 0x0023E8; 
							case 0x000E92: return 0x0023EE; 
							case 0x000E93: return 0x0023F4; 
							case 0x000E94: return 0x0023FA; 
							case 0x000E95: return 0x002400; 
							case 0x000E96: return 0x002406; 
							case 0x000E97: return 0x00240C; 
							case 0x000E98: return 0x002412; 
							case 0x000E99: return 0x002418; 
							case 0x000E9A: return 0x00241E; 
							case 0x000E9B: return 0x002424; 
							case 0x000E9C: return 0x00242A; 
							case 0x000E9D: return 0x002430; 
							case 0x000E9E: return 0x002436; 
							case 0x000E9F: return 0x00243C; 
							case 0x000EA0: return 0x002442; 
							case 0x000EA1: return 0x002448; 
							case 0x000EA2: return 0x00244E; 
							case 0x000EA3: return 0x002454; 
							case 0x000EA5: return 0x00245A; 
							case 0x000EA7: return 0x002460; 
							case 0x000EA8: return 0x002466; 
							case 0x000EA9: return 0x00246C; 
							case 0x000EAA: return 0x0023B8; 
							case 0x000EAB: return 0x002472; 
							case 0x000EAC: return 0x002484; 
							case 0x000EAD: return 0x00248A; 
							case 0x000EAE: return 0x002490; 
							case 0x000EDC: return 0x002478; 
							case 0x000EDD: return 0x00247E; 
							case 0x000EDE: return 0x002388; 
							case 0x000EDF: return 0x0023D0; 
							}

							*w = 1;
							return 0x00249A;
						}
						else if (weight < state_000EC3) {
							if (weight == state_000EC4) {
								switch (u) {
								case 0x000E81: return 0x00238F; 
								case 0x000E82: return 0x002395; 
								case 0x000E84: return 0x00239B; 
								case 0x000E86: return 0x0023A1; 
								case 0x000E87: return 0x0023A7; 
								case 0x000E88: return 0x0023AD; 
								case 0x000E89: return 0x0023B3; 
								case 0x000E8A: return 0x0023BF; 
								case 0x000E8C: return 0x0023C5; 
								case 0x000E8D: return 0x0023D7; 
								case 0x000E8E: return 0x0023CB; 
								case 0x000E8F: return 0x0023DD; 
								case 0x000E90: return 0x0023E3; 
								case 0x000E91: return 0x0023E9; 
								case 0x000E92: return 0x0023EF; 
								case 0x000E93: return 0x0023F5; 
								case 0x000E94: return 0x0023FB; 
								case 0x000E95: return 0x002401; 
								case 0x000E96: return 0x002407; 
								case 0x000E97: return 0x00240D; 
								case 0x000E98: return 0x002413; 
								case 0x000E99: return 0x002419; 
								case 0x000E9A: return 0x00241F; 
								case 0x000E9B: return 0x002425; 
								case 0x000E9C: return 0x00242B; 
								case 0x000E9D: return 0x002431; 
								case 0x000E9E: return 0x002437; 
								case 0x000E9F: return 0x00243D; 
								case 0x000EA0: return 0x002443; 
								case 0x000EA1: return 0x002449; 
								case 0x000EA2: return 0x00244F; 
								case 0x000EA3: return 0x002455; 
								case 0x000EA5: return 0x00245B; 
								case 0x000EA7: return 0x002461; 
								case 0x000EA8: return 0x002467; 
								case 0x000EA9: return 0x00246D; 
								case 0x000EAA: return 0x0023B9; 
								case 0x000EAB: return 0x002473; 
								case 0x000EAC: return 0x002485; 
								case 0x000EAD: return 0x00248B; 
								case 0x000EAE: return 0x002491; 
								case 0x000EDC: return 0x002479; 
								case 0x000EDD: return 0x00247F; 
								case 0x000EDE: return 0x002389; 
								case 0x000EDF: return 0x0023D1; 
								}

								*w = 1;
								return 0x00249B;
							}
						}
					}
				}
				else { /* weight > state_000EC2 */
					if (weight == state_000E44) {
						switch (u) {
						case 0x000E01: return 0x00226B; 
						case 0x000E02: return 0x002271; 
						case 0x000E03: return 0x002277; 
						case 0x000E04: return 0x00227D; 
						case 0x000E05: return 0x002283; 
						case 0x000E06: return 0x002289; 
						case 0x000E07: return 0x00228F; 
						case 0x000E08: return 0x002295; 
						case 0x000E09: return 0x00229B; 
						case 0x000E0A: return 0x0022A1; 
						case 0x000E0B: return 0x0022A7; 
						case 0x000E0C: return 0x0022AD; 
						case 0x000E0D: return 0x0022B3; 
						case 0x000E0E: return 0x0022B9; 
						case 0x000E0F: return 0x0022BF; 
						case 0x000E10: return 0x0022C5; 
						case 0x000E11: return 0x0022CB; 
						case 0x000E12: return 0x0022D1; 
						case 0x000E13: return 0x0022D7; 
						case 0x000E14: return 0x0022DD; 
						case 0x000E15: return 0x0022E3; 
						case 0x000E16: return 0x0022E9; 
						case 0x000E17: return 0x0022EF; 
						case 0x000E18: return 0x0022F5; 
						case 0x000E19: return 0x0022FB; 
						case 0x000E1A: return 0x002301; 
						case 0x000E1B: return 0x002307; 
						case 0x000E1C: return 0x00230D; 
						case 0x000E1D: return 0x002313; 
						case 0x000E1E: return 0x002319; 
						case 0x000E1F: return 0x00231F; 
						case 0x000E20: return 0x002325; 
						case 0x000E21: return 0x00232B; 
						case 0x000E22: return 0x002331; 
						case 0x000E23: return 0x002337; 
						case 0x000E24: return 0x00233D; 
						case 0x000E25: return 0x002343; 
						case 0x000E26: return 0x002349; 
						case 0x000E27: return 0x00234F; 
						case 0x000E28: return 0x002355; 
						case 0x000E29: return 0x00235B; 
						case 0x000E2A: return 0x002361; 
						case 0x000E2B: return 0x002367; 
						case 0x000E2C: return 0x00236D; 
						case 0x000E2D: return 0x002373; 
						case 0x000E2E: return 0x002379; 
						}

						*w = 1;
						return 0x002382;
					}
					else if (weight < state_000E44) {
						if (weight == state_000EC0) {
							switch (u) {
							case 0x000E81: return 0x00238B; 
							case 0x000E82: return 0x002391; 
							case 0x000E84: return 0x002397; 
							case 0x000E86: return 0x00239D; 
							case 0x000E87: return 0x0023A3; 
							case 0x000E88: return 0x0023A9; 
							case 0x000E89: return 0x0023AF; 
							case 0x000E8A: return 0x0023BB; 
							case 0x000E8C: return 0x0023C1; 
							case 0x000E8D: return 0x0023D3; 
							case 0x000E8E: return 0x0023C7; 
							case 0x000E8F: return 0x0023D9; 
							case 0x000E90: return 0x0023DF; 
							case 0x000E91: return 0x0023E5; 
							case 0x000E92: return 0x0023EB; 
							case 0x000E93: return 0x0023F1; 
							case 0x000E94: return 0x0023F7; 
							case 0x000E95: return 0x0023FD; 
							case 0x000E96: return 0x002403; 
							case 0x000E97: return 0x002409; 
							case 0x000E98: return 0x00240F; 
							case 0x000E99: return 0x002415; 
							case 0x000E9A: return 0x00241B; 
							case 0x000E9B: return 0x002421; 
							case 0x000E9C: return 0x002427; 
							case 0x000E9D: return 0x00242D; 
							case 0x000E9E: return 0x002433; 
							case 0x000E9F: return 0x002439; 
							case 0x000EA0: return 0x00243F; 
							case 0x000EA1: return 0x002445; 
							case 0x000EA2: return 0x00244B; 
							case 0x000EA3: return 0x002451; 
							case 0x000EA5: return 0x002457; 
							case 0x000EA7: return 0x00245D; 
							case 0x000EA8: return 0x002463; 
							case 0x000EA9: return 0x002469; 
							case 0x000EAA: return 0x0023B5; 
							case 0x000EAB: return 0x00246F; 
							case 0x000EAC: return 0x002481; 
							case 0x000EAD: return 0x002487; 
							case 0x000EAE: return 0x00248D; 
							case 0x000EDC: return 0x002475; 
							case 0x000EDD: return 0x00247B; 
							case 0x000EDE: return 0x002385; 
							case 0x000EDF: return 0x0023CD; 
							}

							*w = 1;
							return 0x002497;
						}
						else if (weight < state_000EC0) {
							if (weight == state_000EC1) {
								switch (u) {
								case 0x000E81: return 0x00238C; 
								case 0x000E82: return 0x002392; 
								case 0x000E84: return 0x002398; 
								case 0x000E86: return 0x00239E; 
								case 0x000E87: return 0x0023A4; 
								case 0x000E88: return 0x0023AA; 
								case 0x000E89: return 0x0023B0; 
								case 0x000E8A: return 0x0023BC; 
								case 0x000E8C: return 0x0023C2; 
								case 0x000E8D: return 0x0023D4; 
								case 0x000E8E: return 0x0023C8; 
								case 0x000E8F: return 0x0023DA; 
								case 0x000E90: return 0x0023E0; 
								case 0x000E91: return 0x0023E6; 
								case 0x000E92: return 0x0023EC; 
								case 0x000E93: return 0x0023F2; 
								case 0x000E94: return 0x0023F8; 
								case 0x000E95: return 0x0023FE; 
								case 0x000E96: return 0x002404; 
								case 0x000E97: return 0x00240A; 
								case 0x000E98: return 0x002410; 
								case 0x000E99: return 0x002416; 
								case 0x000E9A: return 0x00241C; 
								case 0x000E9B: return 0x002422; 
								case 0x000E9C: return 0x002428; 
								case 0x000E9D: return 0x00242E; 
								case 0x000E9E: return 0x002434; 
								case 0x000E9F: return 0x00243A; 
								case 0x000EA0: return 0x002440; 
								case 0x000EA1: return 0x002446; 
								case 0x000EA2: return 0x00244C; 
								case 0x000EA3: return 0x002452; 
								case 0x000EA5: return 0x002458; 
								case 0x000EA7: return 0x00245E; 
								case 0x000EA8: return 0x002464; 
								case 0x000EA9: return 0x00246A; 
								case 0x000EAA: return 0x0023B6; 
								case 0x000EAB: return 0x002470; 
								case 0x000EAC: return 0x002482; 
								case 0x000EAD: return 0x002488; 
								case 0x000EAE: return 0x00248E; 
								case 0x000EDC: return 0x002476; 
								case 0x000EDD: return 0x00247C; 
								case 0x000EDE: return 0x002386; 
								case 0x000EDF: return 0x0023CE; 
								}

								*w = 1;
								return 0x002498;
							}
						}
					}
					else { /* weight > state_000E44 */
						if (weight == state_000E43) {
							switch (u) {
							case 0x000E01: return 0x00226A; 
							case 0x000E02: return 0x002270; 
							case 0x000E03: return 0x002276; 
							case 0x000E04: return 0x00227C; 
							case 0x000E05: return 0x002282; 
							case 0x000E06: return 0x002288; 
							case 0x000E07: return 0x00228E; 
							case 0x000E08: return 0x002294; 
							case 0x000E09: return 0x00229A; 
							case 0x000E0A: return 0x0022A0; 
							case 0x000E0B: return 0x0022A6; 
							case 0x000E0C: return 0x0022AC; 
							case 0x000E0D: return 0x0022B2; 
							case 0x000E0E: return 0x0022B8; 
							case 0x000E0F: return 0x0022BE; 
							case 0x000E10: return 0x0022C4; 
							case 0x000E11: return 0x0022CA; 
							case 0x000E12: return 0x0022D0; 
							case 0x000E13: return 0x0022D6; 
							case 0x000E14: return 0x0022DC; 
							case 0x000E15: return 0x0022E2; 
							case 0x000E16: return 0x0022E8; 
							case 0x000E17: return 0x0022EE; 
							case 0x000E18: return 0x0022F4; 
							case 0x000E19: return 0x0022FA; 
							case 0x000E1A: return 0x002300; 
							case 0x000E1B: return 0x002306; 
							case 0x000E1C: return 0x00230C; 
							case 0x000E1D: return 0x002312; 
							case 0x000E1E: return 0x002318; 
							case 0x000E1F: return 0x00231E; 
							case 0x000E20: return 0x002324; 
							case 0x000E21: return 0x00232A; 
							case 0x000E22: return 0x002330; 
							case 0x000E23: return 0x002336; 
							case 0x000E24: return 0x00233C; 
							case 0x000E25: return 0x002342; 
							case 0x000E26: return 0x002348; 
							case 0x000E27: return 0x00234E; 
							case 0x000E28: return 0x002354; 
							case 0x000E29: return 0x00235A; 
							case 0x000E2A: return 0x002360; 
							case 0x000E2B: return 0x002366; 
							case 0x000E2C: return 0x00236C; 
							case 0x000E2D: return 0x002372; 
							case 0x000E2E: return 0x002378; 
							}

							*w = 1;
							return 0x002381;
						}
					}
				}
			}
			else { /* weight > state_000E42 */
				if (weight == state_000627) {
					switch (u) {
					case 0x000653: return 0x0014D2; 
					case 0x000654: return 0x0014D5; 
					case 0x000655: return 0x0014DF; 
					}

					*w = 1;
					return 0x00150F;
				}
				else if (weight < state_000627) {
					if (weight == state_000B92) {
						switch (u) {
						case 0x000BD7: return 0x001C89; 
						}

						*w = 1;
						return 0x001C87;
					}
					else if (weight < state_000B92) {
						if (weight == state_000E40) {
							switch (u) {
							case 0x000E01: return 0x002267; 
							case 0x000E02: return 0x00226D; 
							case 0x000E03: return 0x002273; 
							case 0x000E04: return 0x002279; 
							case 0x000E05: return 0x00227F; 
							case 0x000E06: return 0x002285; 
							case 0x000E07: return 0x00228B; 
							case 0x000E08: return 0x002291; 
							case 0x000E09: return 0x002297; 
							case 0x000E0A: return 0x00229D; 
							case 0x000E0B: return 0x0022A3; 
							case 0x000E0C: return 0x0022A9; 
							case 0x000E0D: return 0x0022AF; 
							case 0x000E0E: return 0x0022B5; 
							case 0x000E0F: return 0x0022BB; 
							case 0x000E10: return 0x0022C1; 
							case 0x000E11: return 0x0022C7; 
							case 0x000E12: return 0x0022CD; 
							case 0x000E13: return 0x0022D3; 
							case 0x000E14: return 0x0022D9; 
							case 0x000E15: return 0x0022DF; 
							case 0x000E16: return 0x0022E5; 
							case 0x000E17: return 0x0022EB; 
							case 0x000E18: return 0x0022F1; 
							case 0x000E19: return 0x0022F7; 
							case 0x000E1A: return 0x0022FD; 
							case 0x000E1B: return 0x002303; 
							case 0x000E1C: return 0x002309; 
							case 0x000E1D: return 0x00230F; 
							case 0x000E1E: return 0x002315; 
							case 0x000E1F: return 0x00231B; 
							case 0x000E20: return 0x002321; 
							case 0x000E21: return 0x002327; 
							case 0x000E22: return 0x00232D; 
							case 0x000E23: return 0x002333; 
							case 0x000E24: return 0x002339; 
							case 0x000E25: return 0x00233F; 
							case 0x000E26: return 0x002345; 
							case 0x000E27: return 0x00234B; 
							case 0x000E28: return 0x002351; 
							case 0x000E29: return 0x002357; 
							case 0x000E2A: return 0x00235D; 
							case 0x000E2B: return 0x002363; 
							case 0x000E2C: return 0x002369; 
							case 0x000E2D: return 0x00236F; 
							case 0x000E2E: return 0x002375; 
							}

							*w = 1;
							return 0x00237E;
						}
						else if (weight < state_000E40) {
							if (weight == state_000E41) {
								switch (u) {
								case 0x000E01: return 0x002268; 
								case 0x000E02: return 0x00226E; 
								case 0x000E03: return 0x002274; 
								case 0x000E04: return 0x00227A; 
								case 0x000E05: return 0x002280; 
								case 0x000E06: return 0x002286; 
								case 0x000E07: return 0x00228C; 
								case 0x000E08: return 0x002292; 
								case 0x000E09: return 0x002298; 
								case 0x000E0A: return 0x00229E; 
								case 0x000E0B: return 0x0022A4; 
								case 0x000E0C: return 0x0022AA; 
								case 0x000E0D: return 0x0022B0; 
								case 0x000E0E: return 0x0022B6; 
								case 0x000E0F: return 0x0022BC; 
								case 0x000E10: return 0x0022C2; 
								case 0x000E11: return 0x0022C8; 
								case 0x000E12: return 0x0022CE; 
								case 0x000E13: return 0x0022D4; 
								case 0x000E14: return 0x0022DA; 
								case 0x000E15: return 0x0022E0; 
								case 0x000E16: return 0x0022E6; 
								case 0x000E17: return 0x0022EC; 
								case 0x000E18: return 0x0022F2; 
								case 0x000E19: return 0x0022F8; 
								case 0x000E1A: return 0x0022FE; 
								case 0x000E1B: return 0x002304; 
								case 0x000E1C: return 0x00230A; 
								case 0x000E1D: return 0x002310; 
								case 0x000E1E: return 0x002316; 
								case 0x000E1F: return 0x00231C; 
								case 0x000E20: return 0x002322; 
								case 0x000E21: return 0x002328; 
								case 0x000E22: return 0x00232E; 
								case 0x000E23: return 0x002334; 
								case 0x000E24: return 0x00233A; 
								case 0x000E25: return 0x002340; 
								case 0x000E26: return 0x002346; 
								case 0x000E27: return 0x00234C; 
								case 0x000E28: return 0x002352; 
								case 0x000E29: return 0x002358; 
								case 0x000E2A: return 0x00235E; 
								case 0x000E2B: return 0x002364; 
								case 0x000E2C: return 0x00236A; 
								case 0x000E2D: return 0x002370; 
								case 0x000E2E: return 0x002376; 
								}

								*w = 1;
								return 0x00237F;
							}
						}
					}
					else { /* weight > state_000B92 */
						if (weight == state_000648) {
							switch (u) {
							case 0x000654: return 0x0014DC; 
							}

							*w = 1;
							return 0x001845;
						}
						else if (weight < state_000648) {
							if (weight == state_00064A) {
								switch (u) {
								case 0x000654: return 0x0014E5; 
								}

								*w = 1;
								return 0x00186B;
							}
						}
					}
				}
				else { /* weight > state_000627 */
					if (weight == state_00006C) {
						switch (u) {
						case 0x0000B7: return 0x000C45; 
						case 0x000387: return 0x000C45; 
						}

						*w = 1;
						return 0x000C3C;
					}
					else if (weight < state_00006C) {
						if (weight == state_000418) {
							switch (u) {
							case 0x000306: return 0x0011C2; 
							}

							*w = 1;
							return 0x0011B5;
						}
						else if (weight < state_000418) {
							if (weight == state_000438) {
								switch (u) {
								case 0x000306: return 0x0011C1; 
								}

								*w = 1;
								return 0x0011B1;
							}
						}
					}
					else { /* weight > state_00006C */
						if (weight == state_00004C) {
							switch (u) {
							case 0x0000B7: return 0x000C56; 
							case 0x000387: return 0x000C56; 
							}

							*w = 1;
							return 0x000C4D;
						}
					}
				}
			}
		}
	}

	return 0;
}
