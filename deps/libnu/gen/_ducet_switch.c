/* Automatically generated file (contractions-toc), 1551675559
 *
 * Tag          : _nu_ducet
 * Contractions : 890
 */

#include <stdint.h>

#include "udb.h"

const size_t _NU_DUCET_CONTRACTIONS = 890; /* contractions included in switch */
const size_t _NU_DUCET_CODEPOINTS = 22606; /* complementary codepoints number */

#define state_00AAB9 -907
#define state_000438 -895
#define state_001B09 -803
#define state_0019B6 -801
#define state_0019BA -794
#define state_00AABC -787
#define state_00006C -766
#define state_00064A -732
#define state_000648 -721
#define state_00AABB -714
#define state_001B05 -675
#define state_000418 -534
#define state_001B07 -516
#define state_0019B7 -460
#define state_000627 -412
#define state_000B92 -402
#define state_00004C -361
#define state_001B0D -268
#define state_001025 -232
#define state_000E40 -211
#define state_000E41 -210
#define state_000E42 -209
#define state_000E43 -208
#define state_000E44 -207
#define state_00AAB5 -182
#define state_000EC1 -122
#define state_000EC0 -121
#define state_000EC3 -120
#define state_000EC2 -119
#define state_000EC4 -117
#define state_001B0B -66
#define state_0019B5 -42
#define state_001B11 -30
#define state_00AAB6 -1

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
	0x0169, 0x010C, 0x02D1, 0x0079, 0x02DC, 0x00CF, 0x0042, 0x007A, 0x0078, 0x02A3, 
	0x00D2, 0x00D0, 0x02FE, 0x019C, 0x002A, 0x00E8, 0x0204, 0x038B, 0x00D3, 0x01CC, 
	0x00D1, 0x0313, 0x0323, 0x031A, 0x0077, 0x0321, 0x0192, 0x0216, 0x02CA, 0x037F, 
	0x00B6, 0x0001, 0x001E, 0x0075, };

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

		if (weight == state_001B0D) {
			switch (u) {
			case 0x001B35: return 0x0028B8; 
			}

			*w = 1;
			return 0x0028B7;
		}
		else if (weight < state_001B0D) {
			if (weight == state_000648) {
				switch (u) {
				case 0x000654: return 0x001434; 
				}

				*w = 1;
				return 0x001788;
			}
			else if (weight < state_000648) {
				if (weight == state_0019BA) {
					switch (u) {
					case 0x00198F: return 0x00279F; 
					case 0x0019A2: return 0x0027FE; 
					case 0x001995: return 0x0027BD; 
					case 0x00199C: return 0x0027E0; 
					case 0x001980: return 0x002754; 
					case 0x001991: return 0x0027A9; 
					case 0x0019A7: return 0x002817; 
					case 0x001984: return 0x002768; 
					case 0x00199D: return 0x0027E5; 
					case 0x00198A: return 0x002786; 
					case 0x0019A3: return 0x002803; 
					case 0x001983: return 0x002763; 
					case 0x00198E: return 0x00279A; 
					case 0x001996: return 0x0027C2; 
					case 0x0019A4: return 0x002808; 
					case 0x001987: return 0x002777; 
					case 0x00199E: return 0x0027EA; 
					case 0x001992: return 0x0027AE; 
					case 0x0019A0: return 0x0027F4; 
					case 0x00199A: return 0x0027D6; 
					case 0x001982: return 0x00275E; 
					case 0x00198D: return 0x002795; 
					case 0x0019AA: return 0x002826; 
					case 0x001997: return 0x0027C7; 
					case 0x0019A5: return 0x00280D; 
					case 0x001986: return 0x002772; 
					case 0x0019A8: return 0x00281C; 
					case 0x001989: return 0x002781; 
					case 0x001993: return 0x0027B3; 
					case 0x00198C: return 0x002790; 
					case 0x0019A1: return 0x0027F9; 
					case 0x001998: return 0x0027CC; 
					case 0x00199F: return 0x0027EF; 
					case 0x001994: return 0x0027B8; 
					case 0x00199B: return 0x0027DB; 
					case 0x001981: return 0x002759; 
					case 0x0019A9: return 0x002821; 
					case 0x0019AB: return 0x00282B; 
					case 0x001988: return 0x00277C; 
					case 0x001990: return 0x0027A4; 
					case 0x00198B: return 0x00278B; 
					case 0x0019A6: return 0x002812; 
					case 0x001999: return 0x0027D1; 
					case 0x001985: return 0x00276D; 
					}

					*w = 1;
					return 0x002836;
				}
				else if (weight < state_0019BA) {
					if (weight == state_001B09) {
						switch (u) {
						case 0x001B35: return 0x0028B4; 
						}

						*w = 1;
						return 0x0028B3;
					}
					else if (weight < state_001B09) {
						if (weight == state_000438) {
							switch (u) {
							case 0x000306: return 0x001122; 
							}

							*w = 1;
							return 0x001112;
						}
						else if (weight < state_000438) {
							if (weight == state_00AAB9) {
								switch (u) {
								case 0x00AA92: return 0x002393; 
								case 0x00AAA5: return 0x002405; 
								case 0x00AAAC: return 0x00242F; 
								case 0x00AA8F: return 0x002381; 
								case 0x00AA82: return 0x002333; 
								case 0x00AA9C: return 0x0023CF; 
								case 0x00AAA1: return 0x0023ED; 
								case 0x00AA97: return 0x0023B1; 
								case 0x00AAAD: return 0x002435; 
								case 0x00AA86: return 0x00234B; 
								case 0x00AA93: return 0x002399; 
								case 0x00AA9D: return 0x0023D5; 
								case 0x00AA8A: return 0x002363; 
								case 0x00AAA6: return 0x00240B; 
								case 0x00AA94: return 0x00239F; 
								case 0x00AA8E: return 0x00237B; 
								case 0x00AAAE: return 0x00243B; 
								case 0x00AA81: return 0x00232D; 
								case 0x00AAA2: return 0x0023F3; 
								case 0x00AA90: return 0x002387; 
								case 0x00AA9E: return 0x0023DB; 
								case 0x00AAAA: return 0x002423; 
								case 0x00AA85: return 0x002345; 
								case 0x00AA9A: return 0x0023C3; 
								case 0x00AAA7: return 0x002411; 
								case 0x00AA95: return 0x0023A5; 
								case 0x00AA89: return 0x00235D; 
								case 0x00AA8D: return 0x002375; 
								case 0x00AA80: return 0x002327; 
								case 0x00AA98: return 0x0023B7; 
								case 0x00AAA3: return 0x0023F9; 
								case 0x00AA91: return 0x00238D; 
								case 0x00AAA8: return 0x002417; 
								case 0x00AAAF: return 0x002441; 
								case 0x00AA84: return 0x00233F; 
								case 0x00AA8C: return 0x00236F; 
								case 0x00AA9F: return 0x0023E1; 
								case 0x00AAA4: return 0x0023FF; 
								case 0x00AAAB: return 0x002429; 
								case 0x00AA88: return 0x002357; 
								case 0x00AA83: return 0x002339; 
								case 0x00AA99: return 0x0023BD; 
								case 0x00AA9B: return 0x0023C9; 
								case 0x00AAA0: return 0x0023E7; 
								case 0x00AA96: return 0x0023AB; 
								case 0x00AAA9: return 0x00241D; 
								case 0x00AA87: return 0x002351; 
								case 0x00AA8B: return 0x002369; 
								}

								*w = 1;
								return 0x002447;
							}
						}
					}
					else { /* weight > state_001B09 */
						if (weight == state_0019B6) {
							switch (u) {
							case 0x001995: return 0x0027BB; 
							case 0x0019A3: return 0x002801; 
							case 0x00199A: return 0x0027D4; 
							case 0x001980: return 0x002752; 
							case 0x00198D: return 0x002793; 
							case 0x001989: return 0x00277F; 
							case 0x0019AA: return 0x002824; 
							case 0x0019A7: return 0x002815; 
							case 0x00199E: return 0x0027E8; 
							case 0x001990: return 0x0027A2; 
							case 0x001985: return 0x00276B; 
							case 0x001994: return 0x0027B6; 
							case 0x0019A2: return 0x0027FC; 
							case 0x001981: return 0x002757; 
							case 0x00198E: return 0x002798; 
							case 0x0019A6: return 0x002810; 
							case 0x00199D: return 0x0027E3; 
							case 0x00198A: return 0x002784; 
							case 0x001986: return 0x002770; 
							case 0x001993: return 0x0027B1; 
							case 0x0019A1: return 0x0027F7; 
							case 0x001982: return 0x00275C; 
							case 0x00198F: return 0x00279D; 
							case 0x001997: return 0x0027C5; 
							case 0x0019A5: return 0x00280B; 
							case 0x00199C: return 0x0027DE; 
							case 0x00198B: return 0x002789; 
							case 0x001987: return 0x002775; 
							case 0x0019A9: return 0x00281F; 
							case 0x001992: return 0x0027AC; 
							case 0x0019A0: return 0x0027F2; 
							case 0x001999: return 0x0027CF; 
							case 0x001983: return 0x002761; 
							case 0x001996: return 0x0027C0; 
							case 0x0019A4: return 0x002806; 
							case 0x00199B: return 0x0027D9; 
							case 0x00198C: return 0x00278E; 
							case 0x001988: return 0x00277A; 
							case 0x0019AB: return 0x002829; 
							case 0x0019A8: return 0x00281A; 
							case 0x00199F: return 0x0027ED; 
							case 0x001991: return 0x0027A7; 
							case 0x001998: return 0x0027CA; 
							case 0x001984: return 0x002766; 
							}

							*w = 1;
							return 0x002832;
						}
					}
				}
				else { /* weight > state_0019BA */
					if (weight == state_00006C) {
						switch (u) {
						case 0x0000B7: return 0x000BD5; 
						case 0x000387: return 0x000BD5; 
						}

						*w = 1;
						return 0x000BCC;
					}
					else if (weight < state_00006C) {
						if (weight == state_00AABC) {
							switch (u) {
							case 0x00AA95: return 0x0023A7; 
							case 0x00AAA1: return 0x0023EF; 
							case 0x00AA84: return 0x002341; 
							case 0x00AA98: return 0x0023B9; 
							case 0x00AA91: return 0x00238F; 
							case 0x00AAAC: return 0x002431; 
							case 0x00AA9E: return 0x0023DD; 
							case 0x00AA88: return 0x002359; 
							case 0x00AAA6: return 0x00240D; 
							case 0x00AA8D: return 0x002377; 
							case 0x00AA9A: return 0x0023C5; 
							case 0x00AA96: return 0x0023AD; 
							case 0x00AAA2: return 0x0023F5; 
							case 0x00AA83: return 0x00233B; 
							case 0x00AA99: return 0x0023BF; 
							case 0x00AA92: return 0x002395; 
							case 0x00AAAD: return 0x002437; 
							case 0x00AA9F: return 0x0023E3; 
							case 0x00AA87: return 0x002353; 
							case 0x00AAA7: return 0x002413; 
							case 0x00AA8C: return 0x002371; 
							case 0x00AA9B: return 0x0023CB; 
							case 0x00AA97: return 0x0023B3; 
							case 0x00AAA3: return 0x0023FB; 
							case 0x00AA82: return 0x002335; 
							case 0x00AA93: return 0x00239B; 
							case 0x00AAAE: return 0x00243D; 
							case 0x00AA86: return 0x00234D; 
							case 0x00AAA8: return 0x002419; 
							case 0x00AA8B: return 0x00236B; 
							case 0x00AAAA: return 0x002425; 
							case 0x00AA9C: return 0x0023D1; 
							case 0x00AAA4: return 0x002401; 
							case 0x00AA8F: return 0x002383; 
							case 0x00AA81: return 0x00232F; 
							case 0x00AA94: return 0x0023A1; 
							case 0x00AAAF: return 0x002443; 
							case 0x00AAA0: return 0x0023E9; 
							case 0x00AA85: return 0x002347; 
							case 0x00AAA9: return 0x00241F; 
							case 0x00AA8A: return 0x002365; 
							case 0x00AA90: return 0x002389; 
							case 0x00AAAB: return 0x00242B; 
							case 0x00AA9D: return 0x0023D7; 
							case 0x00AA89: return 0x00235F; 
							case 0x00AAA5: return 0x002407; 
							case 0x00AA8E: return 0x00237D; 
							case 0x00AA80: return 0x002329; 
							}

							*w = 1;
							return 0x00244A;
						}
					}
					else { /* weight > state_00006C */
						if (weight == state_00064A) {
							switch (u) {
							case 0x000654: return 0x00143D; 
							}

							*w = 1;
							return 0x0017B0;
						}
					}
				}
			}
			else { /* weight > state_000648 */
				if (weight == state_0019B7) {
					switch (u) {
					case 0x0019A2: return 0x0027FD; 
					case 0x001999: return 0x0027D0; 
					case 0x001981: return 0x002758; 
					case 0x00198E: return 0x002799; 
					case 0x001988: return 0x00277B; 
					case 0x001994: return 0x0027B7; 
					case 0x0019A6: return 0x002811; 
					case 0x00198A: return 0x002785; 
					case 0x001984: return 0x002767; 
					case 0x00199D: return 0x0027E4; 
					case 0x001991: return 0x0027A8; 
					case 0x0019A3: return 0x002802; 
					case 0x001980: return 0x002753; 
					case 0x00198D: return 0x002794; 
					case 0x001995: return 0x0027BC; 
					case 0x0019A7: return 0x002816; 
					case 0x00199A: return 0x0027D5; 
					case 0x0019AA: return 0x002825; 
					case 0x00199E: return 0x0027E9; 
					case 0x001992: return 0x0027AD; 
					case 0x001987: return 0x002776; 
					case 0x001996: return 0x0027C1; 
					case 0x0019A0: return 0x0027F3; 
					case 0x00199B: return 0x0027DA; 
					case 0x001983: return 0x002762; 
					case 0x0019AB: return 0x00282A; 
					case 0x0019A4: return 0x002807; 
					case 0x00199F: return 0x0027EE; 
					case 0x001993: return 0x0027B2; 
					case 0x00198C: return 0x00278F; 
					case 0x001986: return 0x002771; 
					case 0x0019A8: return 0x00281B; 
					case 0x001997: return 0x0027C6; 
					case 0x0019A1: return 0x0027F8; 
					case 0x00199C: return 0x0027DF; 
					case 0x001998: return 0x0027CB; 
					case 0x001982: return 0x00275D; 
					case 0x00198F: return 0x00279E; 
					case 0x001989: return 0x002780; 
					case 0x0019A5: return 0x00280C; 
					case 0x00198B: return 0x00278A; 
					case 0x001985: return 0x00276C; 
					case 0x0019A9: return 0x002820; 
					case 0x001990: return 0x0027A3; 
					}

					*w = 1;
					return 0x002833;
				}
				else if (weight < state_0019B7) {
					if (weight == state_000418) {
						switch (u) {
						case 0x000306: return 0x001123; 
						}

						*w = 1;
						return 0x001116;
					}
					else if (weight < state_000418) {
						if (weight == state_001B05) {
							switch (u) {
							case 0x001B35: return 0x0028B0; 
							}

							*w = 1;
							return 0x0028AF;
						}
						else if (weight < state_001B05) {
							if (weight == state_00AABB) {
								switch (u) {
								case 0x00AAA0: return 0x0023E8; 
								case 0x00AA8D: return 0x002376; 
								case 0x00AA81: return 0x00232E; 
								case 0x00AA99: return 0x0023BE; 
								case 0x00AA90: return 0x002388; 
								case 0x00AAAF: return 0x002442; 
								case 0x00AA9D: return 0x0023D6; 
								case 0x00AA85: return 0x002346; 
								case 0x00AA8A: return 0x002364; 
								case 0x00AAAB: return 0x00242A; 
								case 0x00AA89: return 0x00235E; 
								case 0x00AAA7: return 0x002412; 
								case 0x00AA8E: return 0x00237C; 
								case 0x00AA82: return 0x002334; 
								case 0x00AA98: return 0x0023B8; 
								case 0x00AA97: return 0x0023B2; 
								case 0x00AAAE: return 0x00243C; 
								case 0x00AAA3: return 0x0023FA; 
								case 0x00AA86: return 0x00234C; 
								case 0x00AA8B: return 0x00236A; 
								case 0x00AA93: return 0x00239A; 
								case 0x00AAAA: return 0x002424; 
								case 0x00AAA6: return 0x00240C; 
								case 0x00AA8F: return 0x002382; 
								case 0x00AA83: return 0x00233A; 
								case 0x00AA9C: return 0x0023D0; 
								case 0x00AA96: return 0x0023AC; 
								case 0x00AAAD: return 0x002436; 
								case 0x00AAA2: return 0x0023F4; 
								case 0x00AA87: return 0x002352; 
								case 0x00AAA9: return 0x00241E; 
								case 0x00AA8C: return 0x002370; 
								case 0x00AA92: return 0x002394; 
								case 0x00AA9F: return 0x0023E2; 
								case 0x00AAA5: return 0x002406; 
								case 0x00AA9B: return 0x0023CA; 
								case 0x00AA95: return 0x0023A6; 
								case 0x00AAA1: return 0x0023EE; 
								case 0x00AA80: return 0x002328; 
								case 0x00AAA8: return 0x002418; 
								case 0x00AA91: return 0x00238E; 
								case 0x00AA9E: return 0x0023DC; 
								case 0x00AA84: return 0x002340; 
								case 0x00AAA4: return 0x002400; 
								case 0x00AAAC: return 0x002430; 
								case 0x00AA9A: return 0x0023C4; 
								case 0x00AA94: return 0x0023A0; 
								case 0x00AA88: return 0x002358; 
								}

								*w = 1;
								return 0x002449;
							}
						}
					}
					else { /* weight > state_000418 */
						if (weight == state_001B07) {
							switch (u) {
							case 0x001B35: return 0x0028B2; 
							}

							*w = 1;
							return 0x0028B1;
						}
					}
				}
				else { /* weight > state_0019B7 */
					if (weight == state_000B92) {
						switch (u) {
						case 0x000BD7: return 0x001BB0; 
						}

						*w = 1;
						return 0x001BAE;
					}
					else if (weight < state_000B92) {
						if (weight == state_000627) {
							switch (u) {
							case 0x000653: return 0x00142A; 
							case 0x000655: return 0x001437; 
							case 0x000654: return 0x00142D; 
							}

							*w = 1;
							return 0x001467;
						}
					}
					else { /* weight > state_000B92 */
						if (weight == state_00004C) {
							switch (u) {
							case 0x000387: return 0x000BE6; 
							case 0x0000B7: return 0x000BE6; 
							}

							*w = 1;
							return 0x000BDD;
						}
					}
				}
			}
		}
		else { /* weight > state_001B0D */
			if (weight == state_000EC0) {
				switch (u) {
				case 0x000E84: return 0x00221F; 
				case 0x000E9D: return 0x0022B5; 
				case 0x000E8A: return 0x002243; 
				case 0x000EAB: return 0x0022F7; 
				case 0x000E88: return 0x002231; 
				case 0x000E99: return 0x00229D; 
				case 0x000E8E: return 0x00224F; 
				case 0x000E96: return 0x00228B; 
				case 0x000EA2: return 0x0022D3; 
				case 0x000E87: return 0x00222B; 
				case 0x000E9E: return 0x0022BB; 
				case 0x000E92: return 0x002273; 
				case 0x000EDE: return 0x00220D; 
				case 0x000EAC: return 0x002309; 
				case 0x000E9A: return 0x0022A3; 
				case 0x000EA7: return 0x0022E5; 
				case 0x000E82: return 0x002219; 
				case 0x000E8D: return 0x00225B; 
				case 0x000E97: return 0x002291; 
				case 0x000EA3: return 0x0022D9; 
				case 0x000EAD: return 0x00230F; 
				case 0x000E86: return 0x002225; 
				case 0x000EA8: return 0x0022EB; 
				case 0x000E93: return 0x002279; 
				case 0x000E8C: return 0x002249; 
				case 0x000EDD: return 0x002303; 
				case 0x000E9F: return 0x0022C1; 
				case 0x000E94: return 0x00227F; 
				case 0x000E9B: return 0x0022A9; 
				case 0x000EA0: return 0x0022C7; 
				case 0x000EAE: return 0x002315; 
				case 0x000E81: return 0x002213; 
				case 0x000EA9: return 0x0022F1; 
				case 0x000E90: return 0x002267; 
				case 0x000EAA: return 0x00223D; 
				case 0x000EA5: return 0x0022DF; 
				case 0x000E8F: return 0x002261; 
				case 0x000EDC: return 0x0022FD; 
				case 0x000E95: return 0x002285; 
				case 0x000E89: return 0x002237; 
				case 0x000E9C: return 0x0022AF; 
				case 0x000EA1: return 0x0022CD; 
				case 0x000E98: return 0x002297; 
				case 0x000E91: return 0x00226D; 
				case 0x000EDF: return 0x002255; 
				}

				*w = 1;
				return 0x00231F;
			}
			else if (weight < state_000EC0) {
				if (weight == state_000E43) {
					switch (u) {
					case 0x000E02: return 0x0020F8; 
					case 0x000E22: return 0x0021B8; 
					case 0x000E13: return 0x00215E; 
					case 0x000E25: return 0x0021CA; 
					case 0x000E2A: return 0x0021E8; 
					case 0x000E06: return 0x002110; 
					case 0x000E0B: return 0x00212E; 
					case 0x000E28: return 0x0021DC; 
					case 0x000E1C: return 0x002194; 
					case 0x000E2E: return 0x002200; 
					case 0x000E20: return 0x0021AC; 
					case 0x000E0F: return 0x002146; 
					case 0x000E01: return 0x0020F2; 
					case 0x000E14: return 0x002164; 
					case 0x000E24: return 0x0021C4; 
					case 0x000E05: return 0x00210A; 
					case 0x000E0A: return 0x002128; 
					case 0x000E10: return 0x00214C; 
					case 0x000E1D: return 0x00219A; 
					case 0x000E2D: return 0x0021FA; 
					case 0x000E09: return 0x002122; 
					case 0x000E0E: return 0x002140; 
					case 0x000E15: return 0x00216A; 
					case 0x000E27: return 0x0021D6; 
					case 0x000E2C: return 0x0021F4; 
					case 0x000E04: return 0x002104; 
					case 0x000E18: return 0x00217C; 
					case 0x000E11: return 0x002152; 
					case 0x000E1E: return 0x0021A0; 
					case 0x000E08: return 0x00211C; 
					case 0x000E23: return 0x0021BE; 
					case 0x000E0D: return 0x00213A; 
					case 0x000E1A: return 0x002188; 
					case 0x000E16: return 0x002170; 
					case 0x000E26: return 0x0021D0; 
					case 0x000E2B: return 0x0021EE; 
					case 0x000E03: return 0x0020FE; 
					case 0x000E19: return 0x002182; 
					case 0x000E29: return 0x0021E2; 
					case 0x000E12: return 0x002158; 
					case 0x000E1F: return 0x0021A6; 
					case 0x000E07: return 0x002116; 
					case 0x000E0C: return 0x002134; 
					case 0x000E1B: return 0x00218E; 
					case 0x000E17: return 0x002176; 
					case 0x000E21: return 0x0021B2; 
					}

					*w = 1;
					return 0x002209;
				}
				else if (weight < state_000E43) {
					if (weight == state_000E41) {
						switch (u) {
						case 0x000E2C: return 0x0021F2; 
						case 0x000E26: return 0x0021CE; 
						case 0x000E14: return 0x002162; 
						case 0x000E01: return 0x0020F0; 
						case 0x000E0E: return 0x00213E; 
						case 0x000E08: return 0x00211A; 
						case 0x000E1D: return 0x002198; 
						case 0x000E11: return 0x002150; 
						case 0x000E0A: return 0x002126; 
						case 0x000E04: return 0x002102; 
						case 0x000E29: return 0x0021E0; 
						case 0x000E2B: return 0x0021EC; 
						case 0x000E25: return 0x0021C8; 
						case 0x000E15: return 0x002168; 
						case 0x000E1A: return 0x002186; 
						case 0x000E0D: return 0x002138; 
						case 0x000E21: return 0x0021B0; 
						case 0x000E2E: return 0x0021FE; 
						case 0x000E1E: return 0x00219E; 
						case 0x000E23: return 0x0021BC; 
						case 0x000E07: return 0x002114; 
						case 0x000E2A: return 0x0021E6; 
						case 0x000E24: return 0x0021C2; 
						case 0x000E12: return 0x002156; 
						case 0x000E03: return 0x0020FC; 
						case 0x000E20: return 0x0021AA; 
						case 0x000E16: return 0x00216E; 
						case 0x000E28: return 0x0021DA; 
						case 0x000E2D: return 0x0021F8; 
						case 0x000E1B: return 0x00218C; 
						case 0x000E0C: return 0x002132; 
						case 0x000E06: return 0x00210E; 
						case 0x000E1F: return 0x0021A4; 
						case 0x000E13: return 0x00215C; 
						case 0x000E18: return 0x00217A; 
						case 0x000E02: return 0x0020F6; 
						case 0x000E0F: return 0x002144; 
						case 0x000E27: return 0x0021D4; 
						case 0x000E17: return 0x002174; 
						case 0x000E1C: return 0x002192; 
						case 0x000E0B: return 0x00212C; 
						case 0x000E09: return 0x002120; 
						case 0x000E22: return 0x0021B6; 
						case 0x000E10: return 0x00214A; 
						case 0x000E19: return 0x002180; 
						case 0x000E05: return 0x002108; 
						}

						*w = 1;
						return 0x002207;
					}
					else if (weight < state_000E41) {
						if (weight == state_000E40) {
							switch (u) {
							case 0x000E04: return 0x002101; 
							case 0x000E27: return 0x0021D3; 
							case 0x000E11: return 0x00214F; 
							case 0x000E2C: return 0x0021F1; 
							case 0x000E1E: return 0x00219D; 
							case 0x000E18: return 0x002179; 
							case 0x000E0D: return 0x002137; 
							case 0x000E01: return 0x0020EF; 
							case 0x000E1A: return 0x002185; 
							case 0x000E14: return 0x002161; 
							case 0x000E05: return 0x002107; 
							case 0x000E0A: return 0x002125; 
							case 0x000E20: return 0x0021A9; 
							case 0x000E10: return 0x002149; 
							case 0x000E29: return 0x0021DF; 
							case 0x000E1D: return 0x002197; 
							case 0x000E0E: return 0x00213D; 
							case 0x000E24: return 0x0021C1; 
							case 0x000E17: return 0x002173; 
							case 0x000E02: return 0x0020F5; 
							case 0x000E2D: return 0x0021F7; 
							case 0x000E21: return 0x0021AF; 
							case 0x000E13: return 0x00215B; 
							case 0x000E22: return 0x0021B5; 
							case 0x000E06: return 0x00210D; 
							case 0x000E0B: return 0x00212B; 
							case 0x000E25: return 0x0021C7; 
							case 0x000E2A: return 0x0021E5; 
							case 0x000E1C: return 0x002191; 
							case 0x000E16: return 0x00216D; 
							case 0x000E0F: return 0x002143; 
							case 0x000E03: return 0x0020FB; 
							case 0x000E2E: return 0x0021FD; 
							case 0x000E08: return 0x002119; 
							case 0x000E12: return 0x002155; 
							case 0x000E1F: return 0x0021A3; 
							case 0x000E07: return 0x002113; 
							case 0x000E0C: return 0x002131; 
							case 0x000E26: return 0x0021CD; 
							case 0x000E2B: return 0x0021EB; 
							case 0x000E1B: return 0x00218B; 
							case 0x000E19: return 0x00217F; 
							case 0x000E23: return 0x0021BB; 
							case 0x000E28: return 0x0021D9; 
							case 0x000E09: return 0x00211F; 
							case 0x000E15: return 0x002167; 
							}

							*w = 1;
							return 0x002206;
						}
						else if (weight < state_000E40) {
							if (weight == state_001025) {
								switch (u) {
								case 0x00102E: return 0x0026A3; 
								}

								*w = 1;
								return 0x0026A2;
							}
						}
					}
					else { /* weight > state_000E41 */
						if (weight == state_000E42) {
							switch (u) {
							case 0x000E1C: return 0x002193; 
							case 0x000E16: return 0x00216F; 
							case 0x000E24: return 0x0021C3; 
							case 0x000E07: return 0x002115; 
							case 0x000E0C: return 0x002133; 
							case 0x000E12: return 0x002157; 
							case 0x000E1F: return 0x0021A5; 
							case 0x000E2D: return 0x0021F9; 
							case 0x000E21: return 0x0021B1; 
							case 0x000E1B: return 0x00218D; 
							case 0x000E15: return 0x002169; 
							case 0x000E25: return 0x0021C9; 
							case 0x000E2A: return 0x0021E7; 
							case 0x000E23: return 0x0021BD; 
							case 0x000E11: return 0x002151; 
							case 0x000E1E: return 0x00219F; 
							case 0x000E2E: return 0x0021FF; 
							case 0x000E04: return 0x002103; 
							case 0x000E1A: return 0x002187; 
							case 0x000E14: return 0x002163; 
							case 0x000E08: return 0x00211B; 
							case 0x000E0D: return 0x002139; 
							case 0x000E01: return 0x0020F1; 
							case 0x000E19: return 0x002181; 
							case 0x000E28: return 0x0021DB; 
							case 0x000E10: return 0x00214B; 
							case 0x000E26: return 0x0021CF; 
							case 0x000E1D: return 0x002199; 
							case 0x000E2B: return 0x0021ED; 
							case 0x000E05: return 0x002109; 
							case 0x000E0A: return 0x002127; 
							case 0x000E09: return 0x002121; 
							case 0x000E0E: return 0x00213F; 
							case 0x000E02: return 0x0020F7; 
							case 0x000E18: return 0x00217B; 
							case 0x000E17: return 0x002175; 
							case 0x000E27: return 0x0021D5; 
							case 0x000E2C: return 0x0021F3; 
							case 0x000E06: return 0x00210F; 
							case 0x000E0B: return 0x00212D; 
							case 0x000E13: return 0x00215D; 
							case 0x000E29: return 0x0021E1; 
							case 0x000E20: return 0x0021AB; 
							case 0x000E22: return 0x0021B7; 
							case 0x000E0F: return 0x002145; 
							case 0x000E03: return 0x0020FD; 
							}

							*w = 1;
							return 0x002208;
						}
					}
				}
				else { /* weight > state_000E43 */
					if (weight == state_00AAB5) {
						switch (u) {
						case 0x00AAA8: return 0x002415; 
						case 0x00AA9F: return 0x0023DF; 
						case 0x00AA93: return 0x002397; 
						case 0x00AAAA: return 0x002421; 
						case 0x00AAA7: return 0x00240F; 
						case 0x00AA82: return 0x002331; 
						case 0x00AA8F: return 0x00237F; 
						case 0x00AA97: return 0x0023AF; 
						case 0x00AAAE: return 0x002439; 
						case 0x00AA9C: return 0x0023CD; 
						case 0x00AAA0: return 0x0023E5; 
						case 0x00AA8B: return 0x002367; 
						case 0x00AAA9: return 0x00241B; 
						case 0x00AA90: return 0x002385; 
						case 0x00AAA4: return 0x0023FD; 
						case 0x00AA85: return 0x002343; 
						case 0x00AA94: return 0x00239D; 
						case 0x00AAAB: return 0x002427; 
						case 0x00AA81: return 0x00232B; 
						case 0x00AAA1: return 0x0023EB; 
						case 0x00AA8E: return 0x002379; 
						case 0x00AA98: return 0x0023B5; 
						case 0x00AAAF: return 0x00243F; 
						case 0x00AA9D: return 0x0023D3; 
						case 0x00AA91: return 0x00238B; 
						case 0x00AAA5: return 0x002403; 
						case 0x00AA8A: return 0x002361; 
						case 0x00AA84: return 0x00233D; 
						case 0x00AA95: return 0x0023A3; 
						case 0x00AA89: return 0x00235B; 
						case 0x00AAAC: return 0x00242D; 
						case 0x00AA9A: return 0x0023C1; 
						case 0x00AA80: return 0x002325; 
						case 0x00AA8D: return 0x002373; 
						case 0x00AA99: return 0x0023BB; 
						case 0x00AA9E: return 0x0023D9; 
						case 0x00AAA2: return 0x0023F1; 
						case 0x00AA87: return 0x00234F; 
						case 0x00AA92: return 0x002391; 
						case 0x00AA88: return 0x002355; 
						case 0x00AAA6: return 0x002409; 
						case 0x00AA83: return 0x002337; 
						case 0x00AA96: return 0x0023A9; 
						case 0x00AAAD: return 0x002433; 
						case 0x00AA9B: return 0x0023C7; 
						case 0x00AAA3: return 0x0023F7; 
						case 0x00AA8C: return 0x00236D; 
						case 0x00AA86: return 0x002349; 
						}

						*w = 1;
						return 0x002445;
					}
					else if (weight < state_00AAB5) {
						if (weight == state_000E44) {
							switch (u) {
							case 0x000E1E: return 0x0021A1; 
							case 0x000E10: return 0x00214D; 
							case 0x000E08: return 0x00211D; 
							case 0x000E01: return 0x0020F3; 
							case 0x000E28: return 0x0021DD; 
							case 0x000E0E: return 0x002141; 
							case 0x000E14: return 0x002165; 
							case 0x000E2B: return 0x0021EF; 
							case 0x000E27: return 0x0021D7; 
							case 0x000E29: return 0x0021E3; 
							case 0x000E0A: return 0x002129; 
							case 0x000E18: return 0x00217D; 
							case 0x000E22: return 0x0021B9; 
							case 0x000E1D: return 0x00219B; 
							case 0x000E13: return 0x00215F; 
							case 0x000E09: return 0x002123; 
							case 0x000E06: return 0x002111; 
							case 0x000E17: return 0x002177; 
							case 0x000E1C: return 0x002195; 
							case 0x000E2C: return 0x0021F5; 
							case 0x000E02: return 0x0020F9; 
							case 0x000E0F: return 0x002147; 
							case 0x000E12: return 0x002159; 
							case 0x000E24: return 0x0021C5; 
							case 0x000E0B: return 0x00212F; 
							case 0x000E07: return 0x002117; 
							case 0x000E16: return 0x002171; 
							case 0x000E20: return 0x0021AD; 
							case 0x000E23: return 0x0021BF; 
							case 0x000E1B: return 0x00218F; 
							case 0x000E2D: return 0x0021FB; 
							case 0x000E03: return 0x0020FF; 
							case 0x000E1F: return 0x0021A7; 
							case 0x000E25: return 0x0021CB; 
							case 0x000E0C: return 0x002135; 
							case 0x000E04: return 0x002105; 
							case 0x000E11: return 0x002153; 
							case 0x000E21: return 0x0021B3; 
							case 0x000E2E: return 0x002201; 
							case 0x000E0D: return 0x00213B; 
							case 0x000E15: return 0x00216B; 
							case 0x000E1A: return 0x002189; 
							case 0x000E2A: return 0x0021E9; 
							case 0x000E26: return 0x0021D1; 
							case 0x000E05: return 0x00210B; 
							case 0x000E19: return 0x002183; 
							}

							*w = 1;
							return 0x00220A;
						}
					}
					else { /* weight > state_00AAB5 */
						if (weight == state_000EC1) {
							switch (u) {
							case 0x000EAD: return 0x002310; 
							case 0x000E97: return 0x002292; 
							case 0x000EA1: return 0x0022CE; 
							case 0x000E9C: return 0x0022B0; 
							case 0x000E82: return 0x00221A; 
							case 0x000E8F: return 0x002262; 
							case 0x000EAC: return 0x00230A; 
							case 0x000EDE: return 0x00220E; 
							case 0x000EA5: return 0x0022E0; 
							case 0x000E92: return 0x002274; 
							case 0x000E87: return 0x00222C; 
							case 0x000EA9: return 0x0022F2; 
							case 0x000E96: return 0x00228C; 
							case 0x000EA0: return 0x0022C8; 
							case 0x000E9B: return 0x0022AA; 
							case 0x000E99: return 0x00229E; 
							case 0x000E88: return 0x002232; 
							case 0x000EAB: return 0x0022F8; 
							case 0x000E9F: return 0x0022C2; 
							case 0x000E8C: return 0x00224A; 
							case 0x000E84: return 0x002220; 
							case 0x000EDF: return 0x002256; 
							case 0x000EA8: return 0x0022EC; 
							case 0x000E91: return 0x00226E; 
							case 0x000EA3: return 0x0022DA; 
							case 0x000E98: return 0x002298; 
							case 0x000E8D: return 0x00225C; 
							case 0x000E89: return 0x002238; 
							case 0x000E95: return 0x002286; 
							case 0x000EDC: return 0x0022FE; 
							case 0x000EA7: return 0x0022E6; 
							case 0x000E9A: return 0x0022A4; 
							case 0x000EAA: return 0x00223E; 
							case 0x000E9E: return 0x0022BC; 
							case 0x000E90: return 0x002268; 
							case 0x000EA2: return 0x0022D4; 
							case 0x000E81: return 0x002214; 
							case 0x000EAE: return 0x002316; 
							case 0x000E8E: return 0x002250; 
							case 0x000E94: return 0x002280; 
							case 0x000E8A: return 0x002244; 
							case 0x000EDD: return 0x002304; 
							case 0x000E9D: return 0x0022B6; 
							case 0x000E93: return 0x00227A; 
							case 0x000E86: return 0x002226; 
							}

							*w = 1;
							return 0x002320;
						}
					}
				}
			}
			else { /* weight > state_000EC0 */
				if (weight == state_001B0B) {
					switch (u) {
					case 0x001B35: return 0x0028B6; 
					}

					*w = 1;
					return 0x0028B5;
				}
				else if (weight < state_001B0B) {
					if (weight == state_000EC2) {
						switch (u) {
						case 0x000E82: return 0x00221B; 
						case 0x000E9B: return 0x0022AB; 
						case 0x000EDD: return 0x002305; 
						case 0x000EAD: return 0x002311; 
						case 0x000E9F: return 0x0022C3; 
						case 0x000EA0: return 0x0022C9; 
						case 0x000E8C: return 0x00224B; 
						case 0x000EA9: return 0x0022F3; 
						case 0x000E90: return 0x002269; 
						case 0x000EAA: return 0x00223F; 
						case 0x000E81: return 0x002215; 
						case 0x000E9C: return 0x0022B1; 
						case 0x000E94: return 0x002281; 
						case 0x000E8F: return 0x002263; 
						case 0x000EDC: return 0x0022FF; 
						case 0x000EAE: return 0x002317; 
						case 0x000EA1: return 0x0022CF; 
						case 0x000E84: return 0x002221; 
						case 0x000E98: return 0x002299; 
						case 0x000E91: return 0x00226F; 
						case 0x000E89: return 0x002239; 
						case 0x000EA5: return 0x0022E1; 
						case 0x000EAB: return 0x0022F9; 
						case 0x000E95: return 0x002287; 
						case 0x000E8E: return 0x002251; 
						case 0x000EA2: return 0x0022D5; 
						case 0x000E99: return 0x00229F; 
						case 0x000E8A: return 0x002245; 
						case 0x000EDF: return 0x002257; 
						case 0x000E92: return 0x002275; 
						case 0x000E88: return 0x002233; 
						case 0x000E9D: return 0x0022B7; 
						case 0x000EAC: return 0x00230B; 
						case 0x000E87: return 0x00222D; 
						case 0x000E96: return 0x00228D; 
						case 0x000E8D: return 0x00225D; 
						case 0x000E9A: return 0x0022A5; 
						case 0x000EA3: return 0x0022DB; 
						case 0x000EDE: return 0x00220F; 
						case 0x000E93: return 0x00227B; 
						case 0x000E9E: return 0x0022BD; 
						case 0x000EA7: return 0x0022E7; 
						case 0x000E86: return 0x002227; 
						case 0x000EA8: return 0x0022ED; 
						case 0x000E97: return 0x002293; 
						}

						*w = 1;
						return 0x002321;
					}
					else if (weight < state_000EC2) {
						if (weight == state_000EC3) {
							switch (u) {
							case 0x000E96: return 0x00228E; 
							case 0x000E8F: return 0x002264; 
							case 0x000EAE: return 0x002318; 
							case 0x000E9C: return 0x0022B2; 
							case 0x000EA1: return 0x0022D0; 
							case 0x000EA8: return 0x0022EE; 
							case 0x000EDE: return 0x002210; 
							case 0x000E91: return 0x002270; 
							case 0x000EA5: return 0x0022E2; 
							case 0x000E84: return 0x002222; 
							case 0x000E95: return 0x002288; 
							case 0x000EAD: return 0x002312; 
							case 0x000E9B: return 0x0022AC; 
							case 0x000EA0: return 0x0022CA; 
							case 0x000E99: return 0x0022A0; 
							case 0x000E8C: return 0x00224C; 
							case 0x000EDF: return 0x002258; 
							case 0x000E90: return 0x00226A; 
							case 0x000E9F: return 0x0022C4; 
							case 0x000E94: return 0x002282; 
							case 0x000E88: return 0x002234; 
							case 0x000EDC: return 0x002300; 
							case 0x000EAC: return 0x00230C; 
							case 0x000E81: return 0x002216; 
							case 0x000E9A: return 0x0022A6; 
							case 0x000E98: return 0x00229A; 
							case 0x000E8D: return 0x00225E; 
							case 0x000E9E: return 0x0022BE; 
							case 0x000EA3: return 0x0022DC; 
							case 0x000E86: return 0x002228; 
							case 0x000E93: return 0x00227C; 
							case 0x000E89: return 0x00223A; 
							case 0x000EA7: return 0x0022E8; 
							case 0x000EAB: return 0x0022FA; 
							case 0x000E82: return 0x00221C; 
							case 0x000E97: return 0x002294; 
							case 0x000E8E: return 0x002252; 
							case 0x000E9D: return 0x0022B8; 
							case 0x000EA2: return 0x0022D6; 
							case 0x000E87: return 0x00222E; 
							case 0x000E8A: return 0x002246; 
							case 0x000EA9: return 0x0022F4; 
							case 0x000EDD: return 0x002306; 
							case 0x000E92: return 0x002276; 
							case 0x000EAA: return 0x002240; 
							}

							*w = 1;
							return 0x002322;
						}
					}
					else { /* weight > state_000EC2 */
						if (weight == state_000EC4) {
							switch (u) {
							case 0x000EDD: return 0x002307; 
							case 0x000EA8: return 0x0022EF; 
							case 0x000E9F: return 0x0022C5; 
							case 0x000E93: return 0x00227D; 
							case 0x000EAA: return 0x002241; 
							case 0x000EA7: return 0x0022E9; 
							case 0x000E82: return 0x00221D; 
							case 0x000E8F: return 0x002265; 
							case 0x000E97: return 0x002295; 
							case 0x000EDC: return 0x002301; 
							case 0x000EAE: return 0x002319; 
							case 0x000E9C: return 0x0022B3; 
							case 0x000EA0: return 0x0022CB; 
							case 0x000EA9: return 0x0022F5; 
							case 0x000E90: return 0x00226B; 
							case 0x000E94: return 0x002283; 
							case 0x000EAB: return 0x0022FB; 
							case 0x000E81: return 0x002217; 
							case 0x000EA1: return 0x0022D1; 
							case 0x000E8E: return 0x002253; 
							case 0x000E98: return 0x00229B; 
							case 0x000EDF: return 0x002259; 
							case 0x000E9D: return 0x0022B9; 
							case 0x000E91: return 0x002271; 
							case 0x000EA5: return 0x0022E3; 
							case 0x000E8A: return 0x002247; 
							case 0x000E84: return 0x002223; 
							case 0x000E95: return 0x002289; 
							case 0x000E89: return 0x00223B; 
							case 0x000EAC: return 0x00230D; 
							case 0x000E9A: return 0x0022A7; 
							case 0x000E8D: return 0x00225F; 
							case 0x000E99: return 0x0022A1; 
							case 0x000E9E: return 0x0022BF; 
							case 0x000EA2: return 0x0022D7; 
							case 0x000E87: return 0x00222F; 
							case 0x000EDE: return 0x002211; 
							case 0x000E92: return 0x002277; 
							case 0x000E88: return 0x002235; 
							case 0x000E96: return 0x00228F; 
							case 0x000EAD: return 0x002313; 
							case 0x000E9B: return 0x0022AD; 
							case 0x000EA3: return 0x0022DD; 
							case 0x000E8C: return 0x00224D; 
							case 0x000E86: return 0x002229; 
							}

							*w = 1;
							return 0x002323;
						}
					}
				}
				else { /* weight > state_001B0B */
					if (weight == state_001B11) {
						switch (u) {
						case 0x001B35: return 0x0028BC; 
						}

						*w = 1;
						return 0x0028BB;
					}
					else if (weight < state_001B11) {
						if (weight == state_0019B5) {
							switch (u) {
							case 0x0019A8: return 0x002819; 
							case 0x00199F: return 0x0027EC; 
							case 0x001993: return 0x0027B0; 
							case 0x0019AA: return 0x002823; 
							case 0x0019A7: return 0x002814; 
							case 0x001982: return 0x00275B; 
							case 0x00198F: return 0x00279C; 
							case 0x001997: return 0x0027C4; 
							case 0x00199C: return 0x0027DD; 
							case 0x0019A0: return 0x0027F1; 
							case 0x00198B: return 0x002788; 
							case 0x0019A9: return 0x00281E; 
							case 0x001990: return 0x0027A1; 
							case 0x0019A4: return 0x002805; 
							case 0x001985: return 0x00276A; 
							case 0x001994: return 0x0027B5; 
							case 0x0019AB: return 0x002828; 
							case 0x001981: return 0x002756; 
							case 0x0019A1: return 0x0027F6; 
							case 0x00198E: return 0x002797; 
							case 0x001998: return 0x0027C9; 
							case 0x00199D: return 0x0027E2; 
							case 0x001991: return 0x0027A6; 
							case 0x0019A5: return 0x00280A; 
							case 0x00198A: return 0x002783; 
							case 0x001984: return 0x002765; 
							case 0x001995: return 0x0027BA; 
							case 0x001989: return 0x00277E; 
							case 0x00199A: return 0x0027D3; 
							case 0x001980: return 0x002751; 
							case 0x00198D: return 0x002792; 
							case 0x001999: return 0x0027CE; 
							case 0x00199E: return 0x0027E7; 
							case 0x0019A2: return 0x0027FB; 
							case 0x001987: return 0x002774; 
							case 0x001992: return 0x0027AB; 
							case 0x001988: return 0x002779; 
							case 0x0019A6: return 0x00280F; 
							case 0x001983: return 0x002760; 
							case 0x001996: return 0x0027BF; 
							case 0x00199B: return 0x0027D8; 
							case 0x0019A3: return 0x002800; 
							case 0x00198C: return 0x00278D; 
							case 0x001986: return 0x00276F; 
							}

							*w = 1;
							return 0x002831;
						}
					}
					else { /* weight > state_001B11 */
						if (weight == state_00AAB6) {
							switch (u) {
							case 0x00AA9D: return 0x0023D4; 
							case 0x00AA87: return 0x002350; 
							case 0x00AA8A: return 0x002362; 
							case 0x00AAA9: return 0x00241C; 
							case 0x00AAAD: return 0x002434; 
							case 0x00AA92: return 0x002392; 
							case 0x00AAA0: return 0x0023E6; 
							case 0x00AA99: return 0x0023BC; 
							case 0x00AA83: return 0x002338; 
							case 0x00AA96: return 0x0023AA; 
							case 0x00AA8F: return 0x002380; 
							case 0x00AAA4: return 0x0023FE; 
							case 0x00AA9C: return 0x0023CE; 
							case 0x00AA88: return 0x002356; 
							case 0x00AA8B: return 0x002368; 
							case 0x00AAA8: return 0x002416; 
							case 0x00AAAC: return 0x00242E; 
							case 0x00AA91: return 0x00238C; 
							case 0x00AA98: return 0x0023B6; 
							case 0x00AA84: return 0x00233E; 
							case 0x00AA95: return 0x0023A4; 
							case 0x00AAA3: return 0x0023F8; 
							case 0x00AA80: return 0x002326; 
							case 0x00AA9B: return 0x0023C8; 
							case 0x00AA89: return 0x00235C; 
							case 0x00AA8C: return 0x00236E; 
							case 0x00AAA7: return 0x002410; 
							case 0x00AAAB: return 0x002428; 
							case 0x00AA90: return 0x002386; 
							case 0x00AA9F: return 0x0023E0; 
							case 0x00AA85: return 0x002344; 
							case 0x00AAAF: return 0x002440; 
							case 0x00AA94: return 0x00239E; 
							case 0x00AAA2: return 0x0023F2; 
							case 0x00AA81: return 0x00232C; 
							case 0x00AA9A: return 0x0023C2; 
							case 0x00AA8D: return 0x002374; 
							case 0x00AAA6: return 0x00240A; 
							case 0x00AAAA: return 0x002422; 
							case 0x00AA9E: return 0x0023DA; 
							case 0x00AA86: return 0x00234A; 
							case 0x00AAAE: return 0x00243A; 
							case 0x00AA93: return 0x002398; 
							case 0x00AAA1: return 0x0023EC; 
							case 0x00AA82: return 0x002332; 
							case 0x00AA97: return 0x0023B0; 
							case 0x00AA8E: return 0x00237A; 
							case 0x00AAA5: return 0x002404; 
							}

							*w = 1;
							return 0x002446;
						}
					}
				}
			}
		}
	}

	return 0;
}
