# Raymond James Enhanced FT.AGGREGATE Example

> Source: [Confluence](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5774410295/Raymond+James+Enhanced+FT.AGGREGATE+Example)

_A detailed example, along with the expected output format._

## Create Index

```
FT.CREATE idx:relationships ON JSON PREFIX 1 relationship: SCHEMA
    $.relationshipName AS relationshipName TEXT SORTABLE
    $.opportunity.id AS opportunityId NUMERIC
    $.opportunity.subType AS subType TAG
    $.opportunity.target AS target TAG
    $.opportunity.hasComments AS hasComments TAG
    $.opportunity.read AS read TAG
    $.opportunity.bestByDate AS bestByDate TEXT
```

## Add Data To Index

Single Doc

```
JSON.SET relationship:271083434 $ '{"relationshipId": 271083434, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 689182214, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2030-01-25"}}'
```

All Docs

```
JSON.SET relationship:271083434 $ '{"relationshipId": 271083434, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 689182214, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2030-01-25"}}'
JSON.SET relationship:226810735 $ '{"relationshipId": 226810735, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 630285737, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2029-04-13"}}'
JSON.SET relationship:240515222 $ '{"relationshipId": 240515222, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 637105834, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2030-11-15"}}'
JSON.SET relationship:220460589 $ '{"relationshipId": 220460589, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 601487332, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2022-07-05"}}'
JSON.SET relationship:226454308 $ '{"relationshipId": 226454308, "relationshipName": "Estate Plan - Zeta", "opportunity": {"id": 608079890, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2020-04-01"}}'
JSON.SET relationship:211347201 $ '{"relationshipId": 211347201, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 692396510, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2022-10-08"}}'
JSON.SET relationship:263306107 $ '{"relationshipId": 263306107, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 649027218, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2021-05-06"}}'
JSON.SET relationship:277658251 $ '{"relationshipId": 277658251, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 616918723, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2024-10-25"}}'
JSON.SET relationship:206927345 $ '{"relationshipId": 206927345, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 671125660, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2027-10-23"}}'
JSON.SET relationship:259136200 $ '{"relationshipId": 259136200, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 685182908, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2029-07-06"}}'
JSON.SET relationship:229033460 $ '{"relationshipId": 229033460, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 604763876, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2027-12-15"}}'
JSON.SET relationship:297788755 $ '{"relationshipId": 297788755, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 617427440, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2021-03-09"}}'
JSON.SET relationship:292547274 $ '{"relationshipId": 292547274, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 638424814, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2022-02-08"}}'
JSON.SET relationship:209464228 $ '{"relationshipId": 209464228, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 675572953, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2027-04-27"}}'
JSON.SET relationship:258284562 $ '{"relationshipId": 258284562, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 662743899, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2029-06-11"}}'
JSON.SET relationship:284535928 $ '{"relationshipId": 284535928, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 616706510, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2023-05-18"}}'
JSON.SET relationship:253594536 $ '{"relationshipId": 253594536, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 640510937, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2027-12-19"}}'
JSON.SET relationship:232548918 $ '{"relationshipId": 232548918, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 654353998, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2024-11-14"}}'
JSON.SET relationship:294287138 $ '{"relationshipId": 294287138, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 678085903, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2021-06-16"}}'
JSON.SET relationship:275969680 $ '{"relationshipId": 275969680, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 690026914, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2023-01-10"}}'
JSON.SET relationship:294565392 $ '{"relationshipId": 294565392, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 655995605, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2023-04-04"}}'
JSON.SET relationship:272187603 $ '{"relationshipId": 272187603, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 699707403, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2020-04-29"}}'
JSON.SET relationship:269345852 $ '{"relationshipId": 269345852, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 667457964, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2023-12-13"}}'
JSON.SET relationship:230528025 $ '{"relationshipId": 230528025, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 681860226, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2023-12-06"}}'
JSON.SET relationship:209147800 $ '{"relationshipId": 209147800, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 607064830, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2022-12-26"}}'
JSON.SET relationship:212755587 $ '{"relationshipId": 212755587, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 672699620, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2026-07-08"}}'
JSON.SET relationship:284935865 $ '{"relationshipId": 284935865, "relationshipName": "401K SEP", "opportunity": {"id": 693346585, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2023-03-21"}}'
JSON.SET relationship:210629006 $ '{"relationshipId": 210629006, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 606566011, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2026-04-11"}}'
JSON.SET relationship:271958981 $ '{"relationshipId": 271958981, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 634603329, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2024-07-28"}}'
JSON.SET relationship:214455631 $ '{"relationshipId": 214455631, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 604998752, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2026-07-21"}}'
JSON.SET relationship:230648683 $ '{"relationshipId": 230648683, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 625740356, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2025-04-12"}}'
JSON.SET relationship:295987974 $ '{"relationshipId": 295987974, "relationshipName": "401K SEP", "opportunity": {"id": 653093679, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2025-07-08"}}'
JSON.SET relationship:229969385 $ '{"relationshipId": 229969385, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 602030572, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2025-02-19"}}'
JSON.SET relationship:269096988 $ '{"relationshipId": 269096988, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 627957777, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2026-10-23"}}'
JSON.SET relationship:268927899 $ '{"relationshipId": 268927899, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 676349694, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2027-04-16"}}'
JSON.SET relationship:292242377 $ '{"relationshipId": 292242377, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 681638146, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2027-03-16"}}'
JSON.SET relationship:212679717 $ '{"relationshipId": 212679717, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 640230045, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2026-07-18"}}'
JSON.SET relationship:238085877 $ '{"relationshipId": 238085877, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 606452284, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2020-08-13"}}'
JSON.SET relationship:268426442 $ '{"relationshipId": 268426442, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 650204327, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2026-08-11"}}'
JSON.SET relationship:281572521 $ '{"relationshipId": 281572521, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 620361017, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2026-05-28"}}'
JSON.SET relationship:259362725 $ '{"relationshipId": 259362725, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 653407582, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2022-07-10"}}'
JSON.SET relationship:218436193 $ '{"relationshipId": 218436193, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 666009725, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2025-11-23"}}'
JSON.SET relationship:271800959 $ '{"relationshipId": 271800959, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 609018181, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2027-01-04"}}'
JSON.SET relationship:269622796 $ '{"relationshipId": 269622796, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 631456710, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2022-07-28"}}'
JSON.SET relationship:201134953 $ '{"relationshipId": 201134953, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 630778604, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2027-12-10"}}'
JSON.SET relationship:262184549 $ '{"relationshipId": 262184549, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 670476225, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2030-10-20"}}'
JSON.SET relationship:246430441 $ '{"relationshipId": 246430441, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 669317185, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2022-12-26"}}'
JSON.SET relationship:227405160 $ '{"relationshipId": 227405160, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 697686849, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2020-11-11"}}'
JSON.SET relationship:293511713 $ '{"relationshipId": 293511713, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 691286229, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2022-05-24"}}'
JSON.SET relationship:232788646 $ '{"relationshipId": 232788646, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 627631411, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2025-10-16"}}'
JSON.SET relationship:211453153 $ '{"relationshipId": 211453153, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 656902653, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2029-11-06"}}'
JSON.SET relationship:281766062 $ '{"relationshipId": 281766062, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 638534179, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2027-11-26"}}'
JSON.SET relationship:223636546 $ '{"relationshipId": 223636546, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 646853314, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2029-03-01"}}'
JSON.SET relationship:287961422 $ '{"relationshipId": 287961422, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 667034064, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2025-07-13"}}'
JSON.SET relationship:216925583 $ '{"relationshipId": 216925583, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 613792577, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2027-05-18"}}'
JSON.SET relationship:236355957 $ '{"relationshipId": 236355957, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 603827166, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2023-04-08"}}'
JSON.SET relationship:246378898 $ '{"relationshipId": 246378898, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 661264688, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2025-09-14"}}'
JSON.SET relationship:221873059 $ '{"relationshipId": 221873059, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 692255636, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2023-04-15"}}'
JSON.SET relationship:208657362 $ '{"relationshipId": 208657362, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 686840781, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2022-01-05"}}'
JSON.SET relationship:222679000 $ '{"relationshipId": 222679000, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 686058308, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2030-01-21"}}'
JSON.SET relationship:218402446 $ '{"relationshipId": 218402446, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 654633227, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2030-08-15"}}'
JSON.SET relationship:220085220 $ '{"relationshipId": 220085220, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 651783342, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2021-04-07"}}'
JSON.SET relationship:220115656 $ '{"relationshipId": 220115656, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 654734721, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2028-12-12"}}'
JSON.SET relationship:280257386 $ '{"relationshipId": 280257386, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 655966708, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2030-08-19"}}'
JSON.SET relationship:234746649 $ '{"relationshipId": 234746649, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 631006347, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2028-03-24"}}'
JSON.SET relationship:203106732 $ '{"relationshipId": 203106732, "relationshipName": "Estate Plan - Zeta", "opportunity": {"id": 670437751, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2023-07-28"}}'
JSON.SET relationship:276563700 $ '{"relationshipId": 276563700, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 666222075, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2023-11-28"}}'
JSON.SET relationship:254821489 $ '{"relationshipId": 254821489, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 689219950, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2029-01-14"}}'
JSON.SET relationship:284233691 $ '{"relationshipId": 284233691, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 648927930, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2026-12-26"}}'
JSON.SET relationship:230788949 $ '{"relationshipId": 230788949, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 686977359, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2026-02-18"}}'
JSON.SET relationship:235996410 $ '{"relationshipId": 235996410, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 602131627, "subType": "ADD_TOD", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2023-01-15"}}'
JSON.SET relationship:255839728 $ '{"relationshipId": 255839728, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 605282848, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2023-05-24"}}'
JSON.SET relationship:294554648 $ '{"relationshipId": 294554648, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 628989578, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2020-12-27"}}'
JSON.SET relationship:235413331 $ '{"relationshipId": 235413331, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 644445075, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2029-04-04"}}'
JSON.SET relationship:230355910 $ '{"relationshipId": 230355910, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 683818231, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2021-06-12"}}'
JSON.SET relationship:238899757 $ '{"relationshipId": 238899757, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 623486776, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2025-09-21"}}'
JSON.SET relationship:202023782 $ '{"relationshipId": 202023782, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 618175593, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2028-12-27"}}'
JSON.SET relationship:232655512 $ '{"relationshipId": 232655512, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 650355053, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2025-09-01"}}'
JSON.SET relationship:202458398 $ '{"relationshipId": 202458398, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 628385478, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2030-08-03"}}'
JSON.SET relationship:293289446 $ '{"relationshipId": 293289446, "relationshipName": "Estate Plan - Zeta", "opportunity": {"id": 603434625, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2028-05-26"}}'
JSON.SET relationship:213086538 $ '{"relationshipId": 213086538, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 615082119, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2021-10-26"}}'
JSON.SET relationship:241908098 $ '{"relationshipId": 241908098, "relationshipName": "401K SEP", "opportunity": {"id": 628998020, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2024-04-20"}}'
JSON.SET relationship:210278382 $ '{"relationshipId": 210278382, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 638378749, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2024-06-23"}}'
JSON.SET relationship:213871650 $ '{"relationshipId": 213871650, "relationshipName": "Education Savings - Beta", "opportunity": {"id": 644087283, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2024-07-18"}}'
JSON.SET relationship:222454640 $ '{"relationshipId": 222454640, "relationshipName": "Estate Plan - Zeta", "opportunity": {"id": 657836406, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2028-10-31"}}'
JSON.SET relationship:242673394 $ '{"relationshipId": 242673394, "relationshipName": "Trust Fund - Wells", "opportunity": {"id": 695181767, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2029-05-25"}}'
JSON.SET relationship:205224494 $ '{"relationshipId": 205224494, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 624009729, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2030-06-18"}}'
JSON.SET relationship:264926756 $ '{"relationshipId": 264926756, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 648106244, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2030-09-15"}}'
JSON.SET relationship:267841916 $ '{"relationshipId": 267841916, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 627470520, "subType": "START_GOAL_PLAN", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2026-02-27"}}'
JSON.SET relationship:255239086 $ '{"relationshipId": 255239086, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 640823267, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2022-07-07"}}'
JSON.SET relationship:278554570 $ '{"relationshipId": 278554570, "relationshipName": "401K SEP", "opportunity": {"id": 650627845, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2023-08-10"}}'
JSON.SET relationship:248387057 $ '{"relationshipId": 248387057, "relationshipName": "Startup Equity - Epsilon", "opportunity": {"id": 616086084, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2027-10-30"}}'
JSON.SET relationship:255118891 $ '{"relationshipId": 255118891, "relationshipName": "Charity Fund - Omega", "opportunity": {"id": 644138416, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2030-02-08"}}'
JSON.SET relationship:224230168 $ '{"relationshipId": 224230168, "relationshipName": "Investment Portfolio - Delta", "opportunity": {"id": 643479973, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "True", "bestByDate": "2021-01-13"}}'
JSON.SET relationship:283929920 $ '{"relationshipId": 283929920, "relationshipName": "401K SEP", "opportunity": {"id": 680725035, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2023-04-12"}}'
JSON.SET relationship:270243179 $ '{"relationshipId": 270243179, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 665790906, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2021-04-08"}}'
JSON.SET relationship:232773244 $ '{"relationshipId": 232773244, "relationshipName": "401K SEP", "opportunity": {"id": 662080659, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2027-07-04"}}'
JSON.SET relationship:205322442 $ '{"relationshipId": 205322442, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 627644512, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "False", "read": "False", "bestByDate": "2022-07-24"}}'
JSON.SET relationship:257348973 $ '{"relationshipId": 257348973, "relationshipName": "Retirement Plan - Alpha", "opportunity": {"id": 658509653, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "True", "hasComments": "True", "read": "True", "bestByDate": "2028-01-10"}}'
JSON.SET relationship:229220952 $ '{"relationshipId": 229220952, "relationshipName": "Health Savings - Gamma", "opportunity": {"id": 625768496, "subType": "ADD_TOD", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2029-08-19"}}'
```

## Aggregate Command (Existing Behaviour)

```
FT.AGGREGATE idx:relationships "*" 
    LOAD * 
    GROUPBY 1 @relationshipName 
    REDUCE TOLIST 1 @opportunityId AS Opportunity_id 
    SORTBY 2 @relationshipName ASC 
    LIMIT 0 5
```

### Output

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "Opportunity_id"
   4) 1) "693346585"
      2) "628998020"
      3) "650627845"
      4) "662080659"
      5) "680725035"
      6) "653093679"
3) 1) "relationshipName"
   2) "529 Plan - Harper"
   3) "Opportunity_id"
   4) 1) "654353998"
      2) "613792577"
      3) "650204327"
      4) "606566011"
      5) "689219950"
      6) "676349694"
      7) "627644512"
      8) "603827166"
      9) "690026914"
      10) "692255636"
4) 1) "relationshipName"
   2) "Charity Fund - Omega"
   3) "Opportunity_id"
   4) 1) "644138416"
      2) "654633227"
      3) "669317185"
      4) "667457964"
      5) "630285737"
      6) "654734721"
      7) "692396510"
      8) "615082119"
      9) "648106244"
      10) "671125660"
      11) "655995605"
5) 1) "relationshipName"
   2) "Education Savings - Beta"
   3) "Opportunity_id"
   4) 1) "625740356"
      2) "644087283"
      3) "605282848"
      4) "651783342"
      5) "609018181"
      6) "628385478"
      7) "616706510"
      8) "685182908"
      9) "617427440"
      10) "644445075"
      11) "656902653"
6) 1) "relationshipName"
   2) "Estate Plan - Zeta"
   3) "Opportunity_id"
   4) 1) "603434625"
      2) "670437751"
      3) "657836406"
      4) "608079890"
```

---

## Enhanced Behaviour

```
FT.AGGREGATE idx:relationships "*" 
  LOAD * 
  GROUPBY 1 @relationshipName 
    REDUCE TOLIST 10 * SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 5 AS all_distinct_docs
  SORTBY 2 @relationshipId DESC
  LIMIT 0 50
```

### Expected Output

```
1) "10"
2) 1) "relationshipName"
   2) "401K SEP"
   3) "all_distinct_docs"
   4) 1) {"relationshipId": 284935865, "relationshipName": "401K SEP", "opportunity": {"id": 693346585, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2023-03-21"}}
      2) {"relationshipId": 241908098, "relationshipName": "401K SEP", "opportunity": {"id": 628998020, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "False", "bestByDate": "2024-04-20"}}
      3) {"relationshipId": 278554570, "relationshipName": "401K SEP", "opportunity": {"id": 650627845, "subType": "CLIENT_ACCESS_ENROLLMENT", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2023-08-10"}}
      4) {"relationshipId": 232773244, "relationshipName": "401K SEP", "opportunity": {"id": 662080659, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "True", "bestByDate": "2027-07-04"}}
      5) {"relationshipId": 283929920, "relationshipName": "401K SEP", "opportunity": {"id": 680725035, "subType": "ADD_TOD", "target": "False", "hasComments": "True", "read": "True", "bestByDate": "2023-04-12"}}
      6) {"relationshipId": 295987974, "relationshipName": "401K SEP", "opportunity": {"id": 653093679, "subType": "START_GOAL_PLAN", "target": "True", "hasComments": "True", "read": "False", "bestByDate": "2025-07-08"}}
3) 1) "relationshipName"
   2) "529 Plan - Harper"
   3) "all_distinct_docs"
   4) 1) {"relationshipId": 232548918, "relationshipName": "529 Plan - Harper", "opportunity": {"id": 654353998, "subType": "ADD_TOD", "target": "False", "hasComments": "False", "read": "False", "bestByDate": "2024-11-14"}}
      2) and so on......
      3) and so on......
      4) .........
      5) .........
```

### Notes

1. Within group, documents should be sorted as per this clause `SORTBY 4 @target DESC @bestByDate ASC`
2. Within group, number of documents returned are controlled by `LIMIT 0 5`
3. Alias for `TOLIST` will be specified after SORTBY and LIMIT (if they are present). We'll fetch the `SORTBY` and `LIMIT` within group using `nargs` specified after `TOLIST`

---

## Enhanced Behavior with ALLOWDUPS - Deduplication OFF

Assume the index contains multiple identical documents (same fields/values) under the same relationshipName.

```
FT.AGGREGATE idx:relationships "*"
  LOAD *
  GROUPBY 1 @relationshipName
    REDUCE TOLIST 11 * ALLOWDUPS
      SORTBY 4 @target DESC @bestByDate ASC
      LIMIT 0 5
      AS top_docs_all
  SORTBY 2 @relationshipName ASC
  LIMIT 0 10
```

### What `ALLOWDUPS` Changes

- **Preserves duplicates**
- Every matching document contributes to the list
- Sorting and limiting still apply **after grouping**

### Result (conceptual)

```
"relationshipName" = "401K SEP"
"top_docs_all" = [
  { "relationshipId": 284935865, ... },
  { "relationshipId": 284935865, ... },   ← duplicate preserved
  { "relationshipId": 241908098, ... },
  { "relationshipId": 241908098, ... },   ← duplicate preserved
  { "relationshipId": 278554570, ... }
]
```
