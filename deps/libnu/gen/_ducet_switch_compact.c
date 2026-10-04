/* Automatically generated file (contractions-toc), 1783791800
 *
 * Tag          : _nu_ducet
 * Contractions : 890
 */

#include <stdint.h>

#include "udb.h"

const size_t _NU_DUCET_CONTRACTIONS = 890; /* contractions included in switch */
const size_t _NU_DUCET_CODEPOINTS = 12345; /* complementary codepoints number */

#define state_00AABC (-876)
#define state_00AABB (-827)
#define state_00AAB9 (-778)
#define state_00AAB6 (-729)
#define state_00AAB5 (-680)
#define state_001B11 (-678)
#define state_001B0D (-676)
#define state_001B0B (-674)
#define state_001B09 (-672)
#define state_001B07 (-670)
#define state_001B05 (-668)
#define state_0019BA (-623)
#define state_0019B7 (-578)
#define state_0019B6 (-533)
#define state_0019B5 (-488)
#define state_001025 (-486)
#define state_000EC4 (-440)
#define state_000EC3 (-394)
#define state_000EC2 (-348)
#define state_000EC1 (-302)
#define state_000EC0 (-256)
#define state_000E44 (-209)
#define state_000E43 (-162)
#define state_000E42 (-115)
#define state_000E41 (-68)
#define state_000E40 (-21)
#define state_000B92 (-19)
#define state_00064A (-17)
#define state_000648 (-15)
#define state_000627 (-11)
#define state_000438 (-9)
#define state_000418 (-7)
#define state_00006C (-4)
#define state_00004C (-1)

/* clang-format off */
const int16_t _NU_DUCET_ROOTS_G[] = {
	0, -34, 0, 0, -33, -32, -31, -30, -29, -28, 2, -26, 
	-15, 0, 0, 3, 0, 0, -14, -13, -12, 8, 1, 8, 
	-10, 0, -7, -6, 5, 20, 4, -4, -2, 0, };
/* clang-format on */

const size_t _NU_DUCET_ROOTS_G_SIZE = sizeof(_NU_DUCET_ROOTS_G) / sizeof(*_NU_DUCET_ROOTS_G);

/* codepoints */
/* clang-format off */
const uint16_t _NU_DUCET_ROOTS_VALUES_C[] = {
	0x00004C, 0x001B0D, 0x000648, 0x000EC0, 0x00064A, 0x000E44, 0x001B0B, 0x000EC1, 
	0x000EC3, 0x001B05, 0x000E41, 0x000E43, 0x00006C, 0x000627, 0x0019B5, 0x001025, 
	0x001B07, 0x00AAB9, 0x000E40, 0x0019B7, 0x000E42, 0x00AABC, 0x001B09, 0x0019BA, 
	0x000EC2, 0x0019B6, 0x000B92, 0x000418, 0x00AABB, 0x000438, 0x00AAB5, 0x00AAB6, 
	0x001B11, 0x000EC4, };
/* clang-format on */

/* indexes */
/* clang-format off */
const uint16_t _NU_DUCET_ROOTS_VALUES_I[] = {
	0x0001, 0x02A4, 0x000F, 0x0100, 0x0011, 0x00D1, 0x02A2, 0x012E, 0x018A, 0x029C, 
	0x0044, 0x00A2, 0x0004, 0x000B, 0x01E8, 0x01E6, 0x029E, 0x030A, 0x0015, 0x0242, 
	0x0073, 0x036C, 0x02A0, 0x026F, 0x015C, 0x0215, 0x0013, 0x0007, 0x033B, 0x0009, 
	0x02A8, 0x02D9, 0x02A6, 0x01B8, };
/* clang-format on */

/* MPH lookup for root codepoints + binary search on balanced tree
 * for intermediate states */
/* clang-format off */
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

		if (weight == state_000EC3) {
			switch (u) {
			case 0x000E81: return 0x001B94; 
			case 0x000E82: return 0x001B9A; 
			case 0x000E84: return 0x001BA0; 
			case 0x000E86: return 0x001BA6; 
			case 0x000E87: return 0x001BAC; 
			case 0x000E88: return 0x001BB2; 
			case 0x000E89: return 0x001BB8; 
			case 0x000E8A: return 0x001BC4; 
			case 0x000E8C: return 0x001BCA; 
			case 0x000E8D: return 0x001BDC; 
			case 0x000E8E: return 0x001BD0; 
			case 0x000E8F: return 0x001BE2; 
			case 0x000E90: return 0x001BE8; 
			case 0x000E91: return 0x001BEE; 
			case 0x000E92: return 0x001BF4; 
			case 0x000E93: return 0x001BFA; 
			case 0x000E94: return 0x001C00; 
			case 0x000E95: return 0x001C06; 
			case 0x000E96: return 0x001C0C; 
			case 0x000E97: return 0x001C12; 
			case 0x000E98: return 0x001C18; 
			case 0x000E99: return 0x001C1E; 
			case 0x000E9A: return 0x001C24; 
			case 0x000E9B: return 0x001C2A; 
			case 0x000E9C: return 0x001C30; 
			case 0x000E9D: return 0x001C36; 
			case 0x000E9E: return 0x001C3C; 
			case 0x000E9F: return 0x001C42; 
			case 0x000EA0: return 0x001C48; 
			case 0x000EA1: return 0x001C4E; 
			case 0x000EA2: return 0x001C54; 
			case 0x000EA3: return 0x001C5A; 
			case 0x000EA5: return 0x001C60; 
			case 0x000EA7: return 0x001C66; 
			case 0x000EA8: return 0x001C6C; 
			case 0x000EA9: return 0x001C72; 
			case 0x000EAA: return 0x001BBE; 
			case 0x000EAB: return 0x001C78; 
			case 0x000EAC: return 0x001C8A; 
			case 0x000EAD: return 0x001C90; 
			case 0x000EAE: return 0x001C96; 
			case 0x000EDC: return 0x001C7E; 
			case 0x000EDD: return 0x001C84; 
			case 0x000EDE: return 0x001B8E; 
			case 0x000EDF: return 0x001BD6; 
			}

			*w = 1;
			return 0x001CA0;
		}
		else if (weight < state_000EC3) {
			if (weight == state_001B09) {
				switch (u) {
				case 0x001B35: return 0x002160; 
				}

				*w = 1;
				return 0x00215F;
			}
			else if (weight < state_001B09) {
				if (weight == state_00AAB5) {
					switch (u) {
					case 0x00AA80: return 0x001CA3; 
					case 0x00AA81: return 0x001CA9; 
					case 0x00AA82: return 0x001CAF; 
					case 0x00AA83: return 0x001CB5; 
					case 0x00AA84: return 0x001CBB; 
					case 0x00AA85: return 0x001CC1; 
					case 0x00AA86: return 0x001CC7; 
					case 0x00AA87: return 0x001CCD; 
					case 0x00AA88: return 0x001CD3; 
					case 0x00AA89: return 0x001CD9; 
					case 0x00AA8A: return 0x001CDF; 
					case 0x00AA8B: return 0x001CE5; 
					case 0x00AA8C: return 0x001CEB; 
					case 0x00AA8D: return 0x001CF1; 
					case 0x00AA8E: return 0x001CF7; 
					case 0x00AA8F: return 0x001CFD; 
					case 0x00AA90: return 0x001D03; 
					case 0x00AA91: return 0x001D09; 
					case 0x00AA92: return 0x001D0F; 
					case 0x00AA93: return 0x001D15; 
					case 0x00AA94: return 0x001D1B; 
					case 0x00AA95: return 0x001D21; 
					case 0x00AA96: return 0x001D27; 
					case 0x00AA97: return 0x001D2D; 
					case 0x00AA98: return 0x001D33; 
					case 0x00AA99: return 0x001D39; 
					case 0x00AA9A: return 0x001D3F; 
					case 0x00AA9B: return 0x001D45; 
					case 0x00AA9C: return 0x001D4B; 
					case 0x00AA9D: return 0x001D51; 
					case 0x00AA9E: return 0x001D57; 
					case 0x00AA9F: return 0x001D5D; 
					case 0x00AAA0: return 0x001D63; 
					case 0x00AAA1: return 0x001D69; 
					case 0x00AAA2: return 0x001D6F; 
					case 0x00AAA3: return 0x001D75; 
					case 0x00AAA4: return 0x001D7B; 
					case 0x00AAA5: return 0x001D81; 
					case 0x00AAA6: return 0x001D87; 
					case 0x00AAA7: return 0x001D8D; 
					case 0x00AAA8: return 0x001D93; 
					case 0x00AAA9: return 0x001D99; 
					case 0x00AAAA: return 0x001D9F; 
					case 0x00AAAB: return 0x001DA5; 
					case 0x00AAAC: return 0x001DAB; 
					case 0x00AAAD: return 0x001DB1; 
					case 0x00AAAE: return 0x001DB7; 
					case 0x00AAAF: return 0x001DBD; 
					}

					*w = 1;
					return 0x001DC3;
				}
				else if (weight < state_00AAB5) {
					if (weight == state_00AAB9) {
						switch (u) {
						case 0x00AA80: return 0x001CA5; 
						case 0x00AA81: return 0x001CAB; 
						case 0x00AA82: return 0x001CB1; 
						case 0x00AA83: return 0x001CB7; 
						case 0x00AA84: return 0x001CBD; 
						case 0x00AA85: return 0x001CC3; 
						case 0x00AA86: return 0x001CC9; 
						case 0x00AA87: return 0x001CCF; 
						case 0x00AA88: return 0x001CD5; 
						case 0x00AA89: return 0x001CDB; 
						case 0x00AA8A: return 0x001CE1; 
						case 0x00AA8B: return 0x001CE7; 
						case 0x00AA8C: return 0x001CED; 
						case 0x00AA8D: return 0x001CF3; 
						case 0x00AA8E: return 0x001CF9; 
						case 0x00AA8F: return 0x001CFF; 
						case 0x00AA90: return 0x001D05; 
						case 0x00AA91: return 0x001D0B; 
						case 0x00AA92: return 0x001D11; 
						case 0x00AA93: return 0x001D17; 
						case 0x00AA94: return 0x001D1D; 
						case 0x00AA95: return 0x001D23; 
						case 0x00AA96: return 0x001D29; 
						case 0x00AA97: return 0x001D2F; 
						case 0x00AA98: return 0x001D35; 
						case 0x00AA99: return 0x001D3B; 
						case 0x00AA9A: return 0x001D41; 
						case 0x00AA9B: return 0x001D47; 
						case 0x00AA9C: return 0x001D4D; 
						case 0x00AA9D: return 0x001D53; 
						case 0x00AA9E: return 0x001D59; 
						case 0x00AA9F: return 0x001D5F; 
						case 0x00AAA0: return 0x001D65; 
						case 0x00AAA1: return 0x001D6B; 
						case 0x00AAA2: return 0x001D71; 
						case 0x00AAA3: return 0x001D77; 
						case 0x00AAA4: return 0x001D7D; 
						case 0x00AAA5: return 0x001D83; 
						case 0x00AAA6: return 0x001D89; 
						case 0x00AAA7: return 0x001D8F; 
						case 0x00AAA8: return 0x001D95; 
						case 0x00AAA9: return 0x001D9B; 
						case 0x00AAAA: return 0x001DA1; 
						case 0x00AAAB: return 0x001DA7; 
						case 0x00AAAC: return 0x001DAD; 
						case 0x00AAAD: return 0x001DB3; 
						case 0x00AAAE: return 0x001DB9; 
						case 0x00AAAF: return 0x001DBF; 
						}

						*w = 1;
						return 0x001DC5;
					}
					else if (weight < state_00AAB9) {
						if (weight == state_00AABB) {
							switch (u) {
							case 0x00AA80: return 0x001CA6; 
							case 0x00AA81: return 0x001CAC; 
							case 0x00AA82: return 0x001CB2; 
							case 0x00AA83: return 0x001CB8; 
							case 0x00AA84: return 0x001CBE; 
							case 0x00AA85: return 0x001CC4; 
							case 0x00AA86: return 0x001CCA; 
							case 0x00AA87: return 0x001CD0; 
							case 0x00AA88: return 0x001CD6; 
							case 0x00AA89: return 0x001CDC; 
							case 0x00AA8A: return 0x001CE2; 
							case 0x00AA8B: return 0x001CE8; 
							case 0x00AA8C: return 0x001CEE; 
							case 0x00AA8D: return 0x001CF4; 
							case 0x00AA8E: return 0x001CFA; 
							case 0x00AA8F: return 0x001D00; 
							case 0x00AA90: return 0x001D06; 
							case 0x00AA91: return 0x001D0C; 
							case 0x00AA92: return 0x001D12; 
							case 0x00AA93: return 0x001D18; 
							case 0x00AA94: return 0x001D1E; 
							case 0x00AA95: return 0x001D24; 
							case 0x00AA96: return 0x001D2A; 
							case 0x00AA97: return 0x001D30; 
							case 0x00AA98: return 0x001D36; 
							case 0x00AA99: return 0x001D3C; 
							case 0x00AA9A: return 0x001D42; 
							case 0x00AA9B: return 0x001D48; 
							case 0x00AA9C: return 0x001D4E; 
							case 0x00AA9D: return 0x001D54; 
							case 0x00AA9E: return 0x001D5A; 
							case 0x00AA9F: return 0x001D60; 
							case 0x00AAA0: return 0x001D66; 
							case 0x00AAA1: return 0x001D6C; 
							case 0x00AAA2: return 0x001D72; 
							case 0x00AAA3: return 0x001D78; 
							case 0x00AAA4: return 0x001D7E; 
							case 0x00AAA5: return 0x001D84; 
							case 0x00AAA6: return 0x001D8A; 
							case 0x00AAA7: return 0x001D90; 
							case 0x00AAA8: return 0x001D96; 
							case 0x00AAA9: return 0x001D9C; 
							case 0x00AAAA: return 0x001DA2; 
							case 0x00AAAB: return 0x001DA8; 
							case 0x00AAAC: return 0x001DAE; 
							case 0x00AAAD: return 0x001DB4; 
							case 0x00AAAE: return 0x001DBA; 
							case 0x00AAAF: return 0x001DC0; 
							}

							*w = 1;
							return 0x001DC7;
						}
						else if (weight < state_00AABB) {
							if (weight == state_00AABC) {
								switch (u) {
								case 0x00AA80: return 0x001CA7; 
								case 0x00AA81: return 0x001CAD; 
								case 0x00AA82: return 0x001CB3; 
								case 0x00AA83: return 0x001CB9; 
								case 0x00AA84: return 0x001CBF; 
								case 0x00AA85: return 0x001CC5; 
								case 0x00AA86: return 0x001CCB; 
								case 0x00AA87: return 0x001CD1; 
								case 0x00AA88: return 0x001CD7; 
								case 0x00AA89: return 0x001CDD; 
								case 0x00AA8A: return 0x001CE3; 
								case 0x00AA8B: return 0x001CE9; 
								case 0x00AA8C: return 0x001CEF; 
								case 0x00AA8D: return 0x001CF5; 
								case 0x00AA8E: return 0x001CFB; 
								case 0x00AA8F: return 0x001D01; 
								case 0x00AA90: return 0x001D07; 
								case 0x00AA91: return 0x001D0D; 
								case 0x00AA92: return 0x001D13; 
								case 0x00AA93: return 0x001D19; 
								case 0x00AA94: return 0x001D1F; 
								case 0x00AA95: return 0x001D25; 
								case 0x00AA96: return 0x001D2B; 
								case 0x00AA97: return 0x001D31; 
								case 0x00AA98: return 0x001D37; 
								case 0x00AA99: return 0x001D3D; 
								case 0x00AA9A: return 0x001D43; 
								case 0x00AA9B: return 0x001D49; 
								case 0x00AA9C: return 0x001D4F; 
								case 0x00AA9D: return 0x001D55; 
								case 0x00AA9E: return 0x001D5B; 
								case 0x00AA9F: return 0x001D61; 
								case 0x00AAA0: return 0x001D67; 
								case 0x00AAA1: return 0x001D6D; 
								case 0x00AAA2: return 0x001D73; 
								case 0x00AAA3: return 0x001D79; 
								case 0x00AAA4: return 0x001D7F; 
								case 0x00AAA5: return 0x001D85; 
								case 0x00AAA6: return 0x001D8B; 
								case 0x00AAA7: return 0x001D91; 
								case 0x00AAA8: return 0x001D97; 
								case 0x00AAA9: return 0x001D9D; 
								case 0x00AAAA: return 0x001DA3; 
								case 0x00AAAB: return 0x001DA9; 
								case 0x00AAAC: return 0x001DAF; 
								case 0x00AAAD: return 0x001DB5; 
								case 0x00AAAE: return 0x001DBB; 
								case 0x00AAAF: return 0x001DC1; 
								}

								*w = 1;
								return 0x001DC8;
							}
						}
					}
					else { /* weight > state_00AAB9 */
						if (weight == state_00AAB6) {
							switch (u) {
							case 0x00AA80: return 0x001CA4; 
							case 0x00AA81: return 0x001CAA; 
							case 0x00AA82: return 0x001CB0; 
							case 0x00AA83: return 0x001CB6; 
							case 0x00AA84: return 0x001CBC; 
							case 0x00AA85: return 0x001CC2; 
							case 0x00AA86: return 0x001CC8; 
							case 0x00AA87: return 0x001CCE; 
							case 0x00AA88: return 0x001CD4; 
							case 0x00AA89: return 0x001CDA; 
							case 0x00AA8A: return 0x001CE0; 
							case 0x00AA8B: return 0x001CE6; 
							case 0x00AA8C: return 0x001CEC; 
							case 0x00AA8D: return 0x001CF2; 
							case 0x00AA8E: return 0x001CF8; 
							case 0x00AA8F: return 0x001CFE; 
							case 0x00AA90: return 0x001D04; 
							case 0x00AA91: return 0x001D0A; 
							case 0x00AA92: return 0x001D10; 
							case 0x00AA93: return 0x001D16; 
							case 0x00AA94: return 0x001D1C; 
							case 0x00AA95: return 0x001D22; 
							case 0x00AA96: return 0x001D28; 
							case 0x00AA97: return 0x001D2E; 
							case 0x00AA98: return 0x001D34; 
							case 0x00AA99: return 0x001D3A; 
							case 0x00AA9A: return 0x001D40; 
							case 0x00AA9B: return 0x001D46; 
							case 0x00AA9C: return 0x001D4C; 
							case 0x00AA9D: return 0x001D52; 
							case 0x00AA9E: return 0x001D58; 
							case 0x00AA9F: return 0x001D5E; 
							case 0x00AAA0: return 0x001D64; 
							case 0x00AAA1: return 0x001D6A; 
							case 0x00AAA2: return 0x001D70; 
							case 0x00AAA3: return 0x001D76; 
							case 0x00AAA4: return 0x001D7C; 
							case 0x00AAA5: return 0x001D82; 
							case 0x00AAA6: return 0x001D88; 
							case 0x00AAA7: return 0x001D8E; 
							case 0x00AAA8: return 0x001D94; 
							case 0x00AAA9: return 0x001D9A; 
							case 0x00AAAA: return 0x001DA0; 
							case 0x00AAAB: return 0x001DA6; 
							case 0x00AAAC: return 0x001DAC; 
							case 0x00AAAD: return 0x001DB2; 
							case 0x00AAAE: return 0x001DB8; 
							case 0x00AAAF: return 0x001DBE; 
							}

							*w = 1;
							return 0x001DC4;
						}
					}
				}
				else { /* weight > state_00AAB5 */
					if (weight == state_001B0D) {
						switch (u) {
						case 0x001B35: return 0x002164; 
						}

						*w = 1;
						return 0x002163;
					}
					else if (weight < state_001B0D) {
						if (weight == state_001B11) {
							switch (u) {
							case 0x001B35: return 0x002168; 
							}

							*w = 1;
							return 0x002167;
						}
					}
					else { /* weight > state_001B0D */
						if (weight == state_001B0B) {
							switch (u) {
							case 0x001B35: return 0x002162; 
							}

							*w = 1;
							return 0x002161;
						}
					}
				}
			}
			else { /* weight > state_001B09 */
				if (weight == state_0019B6) {
					switch (u) {
					case 0x001980: return 0x001FFE; 
					case 0x001981: return 0x002003; 
					case 0x001982: return 0x002008; 
					case 0x001983: return 0x00200D; 
					case 0x001984: return 0x002012; 
					case 0x001985: return 0x002017; 
					case 0x001986: return 0x00201C; 
					case 0x001987: return 0x002021; 
					case 0x001988: return 0x002026; 
					case 0x001989: return 0x00202B; 
					case 0x00198A: return 0x002030; 
					case 0x00198B: return 0x002035; 
					case 0x00198C: return 0x00203A; 
					case 0x00198D: return 0x00203F; 
					case 0x00198E: return 0x002044; 
					case 0x00198F: return 0x002049; 
					case 0x001990: return 0x00204E; 
					case 0x001991: return 0x002053; 
					case 0x001992: return 0x002058; 
					case 0x001993: return 0x00205D; 
					case 0x001994: return 0x002062; 
					case 0x001995: return 0x002067; 
					case 0x001996: return 0x00206C; 
					case 0x001997: return 0x002071; 
					case 0x001998: return 0x002076; 
					case 0x001999: return 0x00207B; 
					case 0x00199A: return 0x002080; 
					case 0x00199B: return 0x002085; 
					case 0x00199C: return 0x00208A; 
					case 0x00199D: return 0x00208F; 
					case 0x00199E: return 0x002094; 
					case 0x00199F: return 0x002099; 
					case 0x0019A0: return 0x00209E; 
					case 0x0019A1: return 0x0020A3; 
					case 0x0019A2: return 0x0020A8; 
					case 0x0019A3: return 0x0020AD; 
					case 0x0019A4: return 0x0020B2; 
					case 0x0019A5: return 0x0020B7; 
					case 0x0019A6: return 0x0020BC; 
					case 0x0019A7: return 0x0020C1; 
					case 0x0019A8: return 0x0020C6; 
					case 0x0019A9: return 0x0020CB; 
					case 0x0019AA: return 0x0020D0; 
					case 0x0019AB: return 0x0020D5; 
					}

					*w = 1;
					return 0x0020DE;
				}
				else if (weight < state_0019B6) {
					if (weight == state_0019BA) {
						switch (u) {
						case 0x001980: return 0x002000; 
						case 0x001981: return 0x002005; 
						case 0x001982: return 0x00200A; 
						case 0x001983: return 0x00200F; 
						case 0x001984: return 0x002014; 
						case 0x001985: return 0x002019; 
						case 0x001986: return 0x00201E; 
						case 0x001987: return 0x002023; 
						case 0x001988: return 0x002028; 
						case 0x001989: return 0x00202D; 
						case 0x00198A: return 0x002032; 
						case 0x00198B: return 0x002037; 
						case 0x00198C: return 0x00203C; 
						case 0x00198D: return 0x002041; 
						case 0x00198E: return 0x002046; 
						case 0x00198F: return 0x00204B; 
						case 0x001990: return 0x002050; 
						case 0x001991: return 0x002055; 
						case 0x001992: return 0x00205A; 
						case 0x001993: return 0x00205F; 
						case 0x001994: return 0x002064; 
						case 0x001995: return 0x002069; 
						case 0x001996: return 0x00206E; 
						case 0x001997: return 0x002073; 
						case 0x001998: return 0x002078; 
						case 0x001999: return 0x00207D; 
						case 0x00199A: return 0x002082; 
						case 0x00199B: return 0x002087; 
						case 0x00199C: return 0x00208C; 
						case 0x00199D: return 0x002091; 
						case 0x00199E: return 0x002096; 
						case 0x00199F: return 0x00209B; 
						case 0x0019A0: return 0x0020A0; 
						case 0x0019A1: return 0x0020A5; 
						case 0x0019A2: return 0x0020AA; 
						case 0x0019A3: return 0x0020AF; 
						case 0x0019A4: return 0x0020B4; 
						case 0x0019A5: return 0x0020B9; 
						case 0x0019A6: return 0x0020BE; 
						case 0x0019A7: return 0x0020C3; 
						case 0x0019A8: return 0x0020C8; 
						case 0x0019A9: return 0x0020CD; 
						case 0x0019AA: return 0x0020D2; 
						case 0x0019AB: return 0x0020D7; 
						}

						*w = 1;
						return 0x0020E2;
					}
					else if (weight < state_0019BA) {
						if (weight == state_001B05) {
							switch (u) {
							case 0x001B35: return 0x00215C; 
							}

							*w = 1;
							return 0x00215B;
						}
						else if (weight < state_001B05) {
							if (weight == state_001B07) {
								switch (u) {
								case 0x001B35: return 0x00215E; 
								}

								*w = 1;
								return 0x00215D;
							}
						}
					}
					else { /* weight > state_0019BA */
						if (weight == state_0019B7) {
							switch (u) {
							case 0x001980: return 0x001FFF; 
							case 0x001981: return 0x002004; 
							case 0x001982: return 0x002009; 
							case 0x001983: return 0x00200E; 
							case 0x001984: return 0x002013; 
							case 0x001985: return 0x002018; 
							case 0x001986: return 0x00201D; 
							case 0x001987: return 0x002022; 
							case 0x001988: return 0x002027; 
							case 0x001989: return 0x00202C; 
							case 0x00198A: return 0x002031; 
							case 0x00198B: return 0x002036; 
							case 0x00198C: return 0x00203B; 
							case 0x00198D: return 0x002040; 
							case 0x00198E: return 0x002045; 
							case 0x00198F: return 0x00204A; 
							case 0x001990: return 0x00204F; 
							case 0x001991: return 0x002054; 
							case 0x001992: return 0x002059; 
							case 0x001993: return 0x00205E; 
							case 0x001994: return 0x002063; 
							case 0x001995: return 0x002068; 
							case 0x001996: return 0x00206D; 
							case 0x001997: return 0x002072; 
							case 0x001998: return 0x002077; 
							case 0x001999: return 0x00207C; 
							case 0x00199A: return 0x002081; 
							case 0x00199B: return 0x002086; 
							case 0x00199C: return 0x00208B; 
							case 0x00199D: return 0x002090; 
							case 0x00199E: return 0x002095; 
							case 0x00199F: return 0x00209A; 
							case 0x0019A0: return 0x00209F; 
							case 0x0019A1: return 0x0020A4; 
							case 0x0019A2: return 0x0020A9; 
							case 0x0019A3: return 0x0020AE; 
							case 0x0019A4: return 0x0020B3; 
							case 0x0019A5: return 0x0020B8; 
							case 0x0019A6: return 0x0020BD; 
							case 0x0019A7: return 0x0020C2; 
							case 0x0019A8: return 0x0020C7; 
							case 0x0019A9: return 0x0020CC; 
							case 0x0019AA: return 0x0020D1; 
							case 0x0019AB: return 0x0020D6; 
							}

							*w = 1;
							return 0x0020DF;
						}
					}
				}
				else { /* weight > state_0019B6 */
					if (weight == state_001025) {
						switch (u) {
						case 0x00102E: return 0x001F98; 
						}

						*w = 1;
						return 0x001F97;
					}
					else if (weight < state_001025) {
						if (weight == state_0019B5) {
							switch (u) {
							case 0x001980: return 0x001FFD; 
							case 0x001981: return 0x002002; 
							case 0x001982: return 0x002007; 
							case 0x001983: return 0x00200C; 
							case 0x001984: return 0x002011; 
							case 0x001985: return 0x002016; 
							case 0x001986: return 0x00201B; 
							case 0x001987: return 0x002020; 
							case 0x001988: return 0x002025; 
							case 0x001989: return 0x00202A; 
							case 0x00198A: return 0x00202F; 
							case 0x00198B: return 0x002034; 
							case 0x00198C: return 0x002039; 
							case 0x00198D: return 0x00203E; 
							case 0x00198E: return 0x002043; 
							case 0x00198F: return 0x002048; 
							case 0x001990: return 0x00204D; 
							case 0x001991: return 0x002052; 
							case 0x001992: return 0x002057; 
							case 0x001993: return 0x00205C; 
							case 0x001994: return 0x002061; 
							case 0x001995: return 0x002066; 
							case 0x001996: return 0x00206B; 
							case 0x001997: return 0x002070; 
							case 0x001998: return 0x002075; 
							case 0x001999: return 0x00207A; 
							case 0x00199A: return 0x00207F; 
							case 0x00199B: return 0x002084; 
							case 0x00199C: return 0x002089; 
							case 0x00199D: return 0x00208E; 
							case 0x00199E: return 0x002093; 
							case 0x00199F: return 0x002098; 
							case 0x0019A0: return 0x00209D; 
							case 0x0019A1: return 0x0020A2; 
							case 0x0019A2: return 0x0020A7; 
							case 0x0019A3: return 0x0020AC; 
							case 0x0019A4: return 0x0020B1; 
							case 0x0019A5: return 0x0020B6; 
							case 0x0019A6: return 0x0020BB; 
							case 0x0019A7: return 0x0020C0; 
							case 0x0019A8: return 0x0020C5; 
							case 0x0019A9: return 0x0020CA; 
							case 0x0019AA: return 0x0020CF; 
							case 0x0019AB: return 0x0020D4; 
							}

							*w = 1;
							return 0x0020DD;
						}
					}
					else { /* weight > state_001025 */
						if (weight == state_000EC4) {
							switch (u) {
							case 0x000E81: return 0x001B95; 
							case 0x000E82: return 0x001B9B; 
							case 0x000E84: return 0x001BA1; 
							case 0x000E86: return 0x001BA7; 
							case 0x000E87: return 0x001BAD; 
							case 0x000E88: return 0x001BB3; 
							case 0x000E89: return 0x001BB9; 
							case 0x000E8A: return 0x001BC5; 
							case 0x000E8C: return 0x001BCB; 
							case 0x000E8D: return 0x001BDD; 
							case 0x000E8E: return 0x001BD1; 
							case 0x000E8F: return 0x001BE3; 
							case 0x000E90: return 0x001BE9; 
							case 0x000E91: return 0x001BEF; 
							case 0x000E92: return 0x001BF5; 
							case 0x000E93: return 0x001BFB; 
							case 0x000E94: return 0x001C01; 
							case 0x000E95: return 0x001C07; 
							case 0x000E96: return 0x001C0D; 
							case 0x000E97: return 0x001C13; 
							case 0x000E98: return 0x001C19; 
							case 0x000E99: return 0x001C1F; 
							case 0x000E9A: return 0x001C25; 
							case 0x000E9B: return 0x001C2B; 
							case 0x000E9C: return 0x001C31; 
							case 0x000E9D: return 0x001C37; 
							case 0x000E9E: return 0x001C3D; 
							case 0x000E9F: return 0x001C43; 
							case 0x000EA0: return 0x001C49; 
							case 0x000EA1: return 0x001C4F; 
							case 0x000EA2: return 0x001C55; 
							case 0x000EA3: return 0x001C5B; 
							case 0x000EA5: return 0x001C61; 
							case 0x000EA7: return 0x001C67; 
							case 0x000EA8: return 0x001C6D; 
							case 0x000EA9: return 0x001C73; 
							case 0x000EAA: return 0x001BBF; 
							case 0x000EAB: return 0x001C79; 
							case 0x000EAC: return 0x001C8B; 
							case 0x000EAD: return 0x001C91; 
							case 0x000EAE: return 0x001C97; 
							case 0x000EDC: return 0x001C7F; 
							case 0x000EDD: return 0x001C85; 
							case 0x000EDE: return 0x001B8F; 
							case 0x000EDF: return 0x001BD7; 
							}

							*w = 1;
							return 0x001CA1;
						}
					}
				}
			}
		}
		else { /* weight > state_000EC3 */
			if (weight == state_000B92) {
				switch (u) {
				case 0x000BD7: return 0x0018BB; 
				}

				*w = 1;
				return 0x0018B9;
			}
			else if (weight < state_000B92) {
				if (weight == state_000E43) {
					switch (u) {
					case 0x000E01: return 0x001A70; 
					case 0x000E02: return 0x001A76; 
					case 0x000E03: return 0x001A7C; 
					case 0x000E04: return 0x001A82; 
					case 0x000E05: return 0x001A88; 
					case 0x000E06: return 0x001A8E; 
					case 0x000E07: return 0x001A94; 
					case 0x000E08: return 0x001A9A; 
					case 0x000E09: return 0x001AA0; 
					case 0x000E0A: return 0x001AA6; 
					case 0x000E0B: return 0x001AAC; 
					case 0x000E0C: return 0x001AB2; 
					case 0x000E0D: return 0x001AB8; 
					case 0x000E0E: return 0x001ABE; 
					case 0x000E0F: return 0x001AC4; 
					case 0x000E10: return 0x001ACA; 
					case 0x000E11: return 0x001AD0; 
					case 0x000E12: return 0x001AD6; 
					case 0x000E13: return 0x001ADC; 
					case 0x000E14: return 0x001AE2; 
					case 0x000E15: return 0x001AE8; 
					case 0x000E16: return 0x001AEE; 
					case 0x000E17: return 0x001AF4; 
					case 0x000E18: return 0x001AFA; 
					case 0x000E19: return 0x001B00; 
					case 0x000E1A: return 0x001B06; 
					case 0x000E1B: return 0x001B0C; 
					case 0x000E1C: return 0x001B12; 
					case 0x000E1D: return 0x001B18; 
					case 0x000E1E: return 0x001B1E; 
					case 0x000E1F: return 0x001B24; 
					case 0x000E20: return 0x001B2A; 
					case 0x000E21: return 0x001B30; 
					case 0x000E22: return 0x001B36; 
					case 0x000E23: return 0x001B3C; 
					case 0x000E24: return 0x001B42; 
					case 0x000E25: return 0x001B48; 
					case 0x000E26: return 0x001B4E; 
					case 0x000E27: return 0x001B54; 
					case 0x000E28: return 0x001B5A; 
					case 0x000E29: return 0x001B60; 
					case 0x000E2A: return 0x001B66; 
					case 0x000E2B: return 0x001B6C; 
					case 0x000E2C: return 0x001B72; 
					case 0x000E2D: return 0x001B78; 
					case 0x000E2E: return 0x001B7E; 
					}

					*w = 1;
					return 0x001B87;
				}
				else if (weight < state_000E43) {
					if (weight == state_000EC0) {
						switch (u) {
						case 0x000E81: return 0x001B91; 
						case 0x000E82: return 0x001B97; 
						case 0x000E84: return 0x001B9D; 
						case 0x000E86: return 0x001BA3; 
						case 0x000E87: return 0x001BA9; 
						case 0x000E88: return 0x001BAF; 
						case 0x000E89: return 0x001BB5; 
						case 0x000E8A: return 0x001BC1; 
						case 0x000E8C: return 0x001BC7; 
						case 0x000E8D: return 0x001BD9; 
						case 0x000E8E: return 0x001BCD; 
						case 0x000E8F: return 0x001BDF; 
						case 0x000E90: return 0x001BE5; 
						case 0x000E91: return 0x001BEB; 
						case 0x000E92: return 0x001BF1; 
						case 0x000E93: return 0x001BF7; 
						case 0x000E94: return 0x001BFD; 
						case 0x000E95: return 0x001C03; 
						case 0x000E96: return 0x001C09; 
						case 0x000E97: return 0x001C0F; 
						case 0x000E98: return 0x001C15; 
						case 0x000E99: return 0x001C1B; 
						case 0x000E9A: return 0x001C21; 
						case 0x000E9B: return 0x001C27; 
						case 0x000E9C: return 0x001C2D; 
						case 0x000E9D: return 0x001C33; 
						case 0x000E9E: return 0x001C39; 
						case 0x000E9F: return 0x001C3F; 
						case 0x000EA0: return 0x001C45; 
						case 0x000EA1: return 0x001C4B; 
						case 0x000EA2: return 0x001C51; 
						case 0x000EA3: return 0x001C57; 
						case 0x000EA5: return 0x001C5D; 
						case 0x000EA7: return 0x001C63; 
						case 0x000EA8: return 0x001C69; 
						case 0x000EA9: return 0x001C6F; 
						case 0x000EAA: return 0x001BBB; 
						case 0x000EAB: return 0x001C75; 
						case 0x000EAC: return 0x001C87; 
						case 0x000EAD: return 0x001C8D; 
						case 0x000EAE: return 0x001C93; 
						case 0x000EDC: return 0x001C7B; 
						case 0x000EDD: return 0x001C81; 
						case 0x000EDE: return 0x001B8B; 
						case 0x000EDF: return 0x001BD3; 
						}

						*w = 1;
						return 0x001C9D;
					}
					else if (weight < state_000EC0) {
						if (weight == state_000EC1) {
							switch (u) {
							case 0x000E81: return 0x001B92; 
							case 0x000E82: return 0x001B98; 
							case 0x000E84: return 0x001B9E; 
							case 0x000E86: return 0x001BA4; 
							case 0x000E87: return 0x001BAA; 
							case 0x000E88: return 0x001BB0; 
							case 0x000E89: return 0x001BB6; 
							case 0x000E8A: return 0x001BC2; 
							case 0x000E8C: return 0x001BC8; 
							case 0x000E8D: return 0x001BDA; 
							case 0x000E8E: return 0x001BCE; 
							case 0x000E8F: return 0x001BE0; 
							case 0x000E90: return 0x001BE6; 
							case 0x000E91: return 0x001BEC; 
							case 0x000E92: return 0x001BF2; 
							case 0x000E93: return 0x001BF8; 
							case 0x000E94: return 0x001BFE; 
							case 0x000E95: return 0x001C04; 
							case 0x000E96: return 0x001C0A; 
							case 0x000E97: return 0x001C10; 
							case 0x000E98: return 0x001C16; 
							case 0x000E99: return 0x001C1C; 
							case 0x000E9A: return 0x001C22; 
							case 0x000E9B: return 0x001C28; 
							case 0x000E9C: return 0x001C2E; 
							case 0x000E9D: return 0x001C34; 
							case 0x000E9E: return 0x001C3A; 
							case 0x000E9F: return 0x001C40; 
							case 0x000EA0: return 0x001C46; 
							case 0x000EA1: return 0x001C4C; 
							case 0x000EA2: return 0x001C52; 
							case 0x000EA3: return 0x001C58; 
							case 0x000EA5: return 0x001C5E; 
							case 0x000EA7: return 0x001C64; 
							case 0x000EA8: return 0x001C6A; 
							case 0x000EA9: return 0x001C70; 
							case 0x000EAA: return 0x001BBC; 
							case 0x000EAB: return 0x001C76; 
							case 0x000EAC: return 0x001C88; 
							case 0x000EAD: return 0x001C8E; 
							case 0x000EAE: return 0x001C94; 
							case 0x000EDC: return 0x001C7C; 
							case 0x000EDD: return 0x001C82; 
							case 0x000EDE: return 0x001B8C; 
							case 0x000EDF: return 0x001BD4; 
							}

							*w = 1;
							return 0x001C9E;
						}
						else if (weight < state_000EC1) {
							if (weight == state_000EC2) {
								switch (u) {
								case 0x000E81: return 0x001B93; 
								case 0x000E82: return 0x001B99; 
								case 0x000E84: return 0x001B9F; 
								case 0x000E86: return 0x001BA5; 
								case 0x000E87: return 0x001BAB; 
								case 0x000E88: return 0x001BB1; 
								case 0x000E89: return 0x001BB7; 
								case 0x000E8A: return 0x001BC3; 
								case 0x000E8C: return 0x001BC9; 
								case 0x000E8D: return 0x001BDB; 
								case 0x000E8E: return 0x001BCF; 
								case 0x000E8F: return 0x001BE1; 
								case 0x000E90: return 0x001BE7; 
								case 0x000E91: return 0x001BED; 
								case 0x000E92: return 0x001BF3; 
								case 0x000E93: return 0x001BF9; 
								case 0x000E94: return 0x001BFF; 
								case 0x000E95: return 0x001C05; 
								case 0x000E96: return 0x001C0B; 
								case 0x000E97: return 0x001C11; 
								case 0x000E98: return 0x001C17; 
								case 0x000E99: return 0x001C1D; 
								case 0x000E9A: return 0x001C23; 
								case 0x000E9B: return 0x001C29; 
								case 0x000E9C: return 0x001C2F; 
								case 0x000E9D: return 0x001C35; 
								case 0x000E9E: return 0x001C3B; 
								case 0x000E9F: return 0x001C41; 
								case 0x000EA0: return 0x001C47; 
								case 0x000EA1: return 0x001C4D; 
								case 0x000EA2: return 0x001C53; 
								case 0x000EA3: return 0x001C59; 
								case 0x000EA5: return 0x001C5F; 
								case 0x000EA7: return 0x001C65; 
								case 0x000EA8: return 0x001C6B; 
								case 0x000EA9: return 0x001C71; 
								case 0x000EAA: return 0x001BBD; 
								case 0x000EAB: return 0x001C77; 
								case 0x000EAC: return 0x001C89; 
								case 0x000EAD: return 0x001C8F; 
								case 0x000EAE: return 0x001C95; 
								case 0x000EDC: return 0x001C7D; 
								case 0x000EDD: return 0x001C83; 
								case 0x000EDE: return 0x001B8D; 
								case 0x000EDF: return 0x001BD5; 
								}

								*w = 1;
								return 0x001C9F;
							}
						}
					}
					else { /* weight > state_000EC0 */
						if (weight == state_000E44) {
							switch (u) {
							case 0x000E01: return 0x001A71; 
							case 0x000E02: return 0x001A77; 
							case 0x000E03: return 0x001A7D; 
							case 0x000E04: return 0x001A83; 
							case 0x000E05: return 0x001A89; 
							case 0x000E06: return 0x001A8F; 
							case 0x000E07: return 0x001A95; 
							case 0x000E08: return 0x001A9B; 
							case 0x000E09: return 0x001AA1; 
							case 0x000E0A: return 0x001AA7; 
							case 0x000E0B: return 0x001AAD; 
							case 0x000E0C: return 0x001AB3; 
							case 0x000E0D: return 0x001AB9; 
							case 0x000E0E: return 0x001ABF; 
							case 0x000E0F: return 0x001AC5; 
							case 0x000E10: return 0x001ACB; 
							case 0x000E11: return 0x001AD1; 
							case 0x000E12: return 0x001AD7; 
							case 0x000E13: return 0x001ADD; 
							case 0x000E14: return 0x001AE3; 
							case 0x000E15: return 0x001AE9; 
							case 0x000E16: return 0x001AEF; 
							case 0x000E17: return 0x001AF5; 
							case 0x000E18: return 0x001AFB; 
							case 0x000E19: return 0x001B01; 
							case 0x000E1A: return 0x001B07; 
							case 0x000E1B: return 0x001B0D; 
							case 0x000E1C: return 0x001B13; 
							case 0x000E1D: return 0x001B19; 
							case 0x000E1E: return 0x001B1F; 
							case 0x000E1F: return 0x001B25; 
							case 0x000E20: return 0x001B2B; 
							case 0x000E21: return 0x001B31; 
							case 0x000E22: return 0x001B37; 
							case 0x000E23: return 0x001B3D; 
							case 0x000E24: return 0x001B43; 
							case 0x000E25: return 0x001B49; 
							case 0x000E26: return 0x001B4F; 
							case 0x000E27: return 0x001B55; 
							case 0x000E28: return 0x001B5B; 
							case 0x000E29: return 0x001B61; 
							case 0x000E2A: return 0x001B67; 
							case 0x000E2B: return 0x001B6D; 
							case 0x000E2C: return 0x001B73; 
							case 0x000E2D: return 0x001B79; 
							case 0x000E2E: return 0x001B7F; 
							}

							*w = 1;
							return 0x001B88;
						}
					}
				}
				else { /* weight > state_000E43 */
					if (weight == state_000E41) {
						switch (u) {
						case 0x000E01: return 0x001A6E; 
						case 0x000E02: return 0x001A74; 
						case 0x000E03: return 0x001A7A; 
						case 0x000E04: return 0x001A80; 
						case 0x000E05: return 0x001A86; 
						case 0x000E06: return 0x001A8C; 
						case 0x000E07: return 0x001A92; 
						case 0x000E08: return 0x001A98; 
						case 0x000E09: return 0x001A9E; 
						case 0x000E0A: return 0x001AA4; 
						case 0x000E0B: return 0x001AAA; 
						case 0x000E0C: return 0x001AB0; 
						case 0x000E0D: return 0x001AB6; 
						case 0x000E0E: return 0x001ABC; 
						case 0x000E0F: return 0x001AC2; 
						case 0x000E10: return 0x001AC8; 
						case 0x000E11: return 0x001ACE; 
						case 0x000E12: return 0x001AD4; 
						case 0x000E13: return 0x001ADA; 
						case 0x000E14: return 0x001AE0; 
						case 0x000E15: return 0x001AE6; 
						case 0x000E16: return 0x001AEC; 
						case 0x000E17: return 0x001AF2; 
						case 0x000E18: return 0x001AF8; 
						case 0x000E19: return 0x001AFE; 
						case 0x000E1A: return 0x001B04; 
						case 0x000E1B: return 0x001B0A; 
						case 0x000E1C: return 0x001B10; 
						case 0x000E1D: return 0x001B16; 
						case 0x000E1E: return 0x001B1C; 
						case 0x000E1F: return 0x001B22; 
						case 0x000E20: return 0x001B28; 
						case 0x000E21: return 0x001B2E; 
						case 0x000E22: return 0x001B34; 
						case 0x000E23: return 0x001B3A; 
						case 0x000E24: return 0x001B40; 
						case 0x000E25: return 0x001B46; 
						case 0x000E26: return 0x001B4C; 
						case 0x000E27: return 0x001B52; 
						case 0x000E28: return 0x001B58; 
						case 0x000E29: return 0x001B5E; 
						case 0x000E2A: return 0x001B64; 
						case 0x000E2B: return 0x001B6A; 
						case 0x000E2C: return 0x001B70; 
						case 0x000E2D: return 0x001B76; 
						case 0x000E2E: return 0x001B7C; 
						}

						*w = 1;
						return 0x001B85;
					}
					else if (weight < state_000E41) {
						if (weight == state_000E42) {
							switch (u) {
							case 0x000E01: return 0x001A6F; 
							case 0x000E02: return 0x001A75; 
							case 0x000E03: return 0x001A7B; 
							case 0x000E04: return 0x001A81; 
							case 0x000E05: return 0x001A87; 
							case 0x000E06: return 0x001A8D; 
							case 0x000E07: return 0x001A93; 
							case 0x000E08: return 0x001A99; 
							case 0x000E09: return 0x001A9F; 
							case 0x000E0A: return 0x001AA5; 
							case 0x000E0B: return 0x001AAB; 
							case 0x000E0C: return 0x001AB1; 
							case 0x000E0D: return 0x001AB7; 
							case 0x000E0E: return 0x001ABD; 
							case 0x000E0F: return 0x001AC3; 
							case 0x000E10: return 0x001AC9; 
							case 0x000E11: return 0x001ACF; 
							case 0x000E12: return 0x001AD5; 
							case 0x000E13: return 0x001ADB; 
							case 0x000E14: return 0x001AE1; 
							case 0x000E15: return 0x001AE7; 
							case 0x000E16: return 0x001AED; 
							case 0x000E17: return 0x001AF3; 
							case 0x000E18: return 0x001AF9; 
							case 0x000E19: return 0x001AFF; 
							case 0x000E1A: return 0x001B05; 
							case 0x000E1B: return 0x001B0B; 
							case 0x000E1C: return 0x001B11; 
							case 0x000E1D: return 0x001B17; 
							case 0x000E1E: return 0x001B1D; 
							case 0x000E1F: return 0x001B23; 
							case 0x000E20: return 0x001B29; 
							case 0x000E21: return 0x001B2F; 
							case 0x000E22: return 0x001B35; 
							case 0x000E23: return 0x001B3B; 
							case 0x000E24: return 0x001B41; 
							case 0x000E25: return 0x001B47; 
							case 0x000E26: return 0x001B4D; 
							case 0x000E27: return 0x001B53; 
							case 0x000E28: return 0x001B59; 
							case 0x000E29: return 0x001B5F; 
							case 0x000E2A: return 0x001B65; 
							case 0x000E2B: return 0x001B6B; 
							case 0x000E2C: return 0x001B71; 
							case 0x000E2D: return 0x001B77; 
							case 0x000E2E: return 0x001B7D; 
							}

							*w = 1;
							return 0x001B86;
						}
					}
					else { /* weight > state_000E41 */
						if (weight == state_000E40) {
							switch (u) {
							case 0x000E01: return 0x001A6D; 
							case 0x000E02: return 0x001A73; 
							case 0x000E03: return 0x001A79; 
							case 0x000E04: return 0x001A7F; 
							case 0x000E05: return 0x001A85; 
							case 0x000E06: return 0x001A8B; 
							case 0x000E07: return 0x001A91; 
							case 0x000E08: return 0x001A97; 
							case 0x000E09: return 0x001A9D; 
							case 0x000E0A: return 0x001AA3; 
							case 0x000E0B: return 0x001AA9; 
							case 0x000E0C: return 0x001AAF; 
							case 0x000E0D: return 0x001AB5; 
							case 0x000E0E: return 0x001ABB; 
							case 0x000E0F: return 0x001AC1; 
							case 0x000E10: return 0x001AC7; 
							case 0x000E11: return 0x001ACD; 
							case 0x000E12: return 0x001AD3; 
							case 0x000E13: return 0x001AD9; 
							case 0x000E14: return 0x001ADF; 
							case 0x000E15: return 0x001AE5; 
							case 0x000E16: return 0x001AEB; 
							case 0x000E17: return 0x001AF1; 
							case 0x000E18: return 0x001AF7; 
							case 0x000E19: return 0x001AFD; 
							case 0x000E1A: return 0x001B03; 
							case 0x000E1B: return 0x001B09; 
							case 0x000E1C: return 0x001B0F; 
							case 0x000E1D: return 0x001B15; 
							case 0x000E1E: return 0x001B1B; 
							case 0x000E1F: return 0x001B21; 
							case 0x000E20: return 0x001B27; 
							case 0x000E21: return 0x001B2D; 
							case 0x000E22: return 0x001B33; 
							case 0x000E23: return 0x001B39; 
							case 0x000E24: return 0x001B3F; 
							case 0x000E25: return 0x001B45; 
							case 0x000E26: return 0x001B4B; 
							case 0x000E27: return 0x001B51; 
							case 0x000E28: return 0x001B57; 
							case 0x000E29: return 0x001B5D; 
							case 0x000E2A: return 0x001B63; 
							case 0x000E2B: return 0x001B69; 
							case 0x000E2C: return 0x001B6F; 
							case 0x000E2D: return 0x001B75; 
							case 0x000E2E: return 0x001B7B; 
							}

							*w = 1;
							return 0x001B84;
						}
					}
				}
			}
			else { /* weight > state_000B92 */
				if (weight == state_000438) {
					switch (u) {
					case 0x000306: return 0x000E70; 
					}

					*w = 1;
					return 0x000E60;
				}
				else if (weight < state_000438) {
					if (weight == state_000648) {
						switch (u) {
						case 0x000654: return 0x00114F; 
						}

						*w = 1;
						return 0x001496;
					}
					else if (weight < state_000648) {
						if (weight == state_00064A) {
							switch (u) {
							case 0x000654: return 0x001158; 
							}

							*w = 1;
							return 0x0014BB;
						}
					}
					else { /* weight > state_000648 */
						if (weight == state_000627) {
							switch (u) {
							case 0x000653: return 0x001145; 
							case 0x000654: return 0x001148; 
							case 0x000655: return 0x001152; 
							}

							*w = 1;
							return 0x001182;
						}
					}
				}
				else { /* weight > state_000438 */
					if (weight == state_00006C) {
						switch (u) {
						case 0x0000B7: return 0x000952; 
						case 0x000387: return 0x000952; 
						}

						*w = 1;
						return 0x000949;
					}
					else if (weight < state_00006C) {
						if (weight == state_000418) {
							switch (u) {
							case 0x000306: return 0x000E71; 
							}

							*w = 1;
							return 0x000E64;
						}
					}
					else { /* weight > state_00006C */
						if (weight == state_00004C) {
							switch (u) {
							case 0x0000B7: return 0x000963; 
							case 0x000387: return 0x000963; 
							}

							*w = 1;
							return 0x00095A;
						}
					}
				}
			}
		}
	}

	return 0;
}
/* clang-format on */
