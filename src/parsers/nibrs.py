class BaseSegmentBHParser:
    def __init__(self, line: str):
        self.line = line
        self.record = {}

    def unpack_common_fields(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
        }


class SegmentB1Parser(BaseSegmentBHParser):
    def __init__(self, line: str):
        super().__init__(line)
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        record = self.unpack_common_fields()
        record.update(
            {
                "segment": self.line[0:2],
                "state_code": self.line[2:4],
                "ori": self.line[4:13],
                "incident_no": self.line[13:25],
                "ori_added": self.line[25:33],
                "ori_nibrs": self.line[33:41],
                "city": self.line[41:71],
                "state_abbr": self.line[71:73],
                "pop_group": self.line[73:75],
                "division": self.line[75:76],
                "region": self.line[76:77],
                "agency_ind": self.line[77:78],
                "core_city": self.line[78:79],
                "covered_by_ori": self.line[79:88],
                "fbi_field": self.line[89:92],
                "judicial_dist": self.line[92:96],
                "nibrs_flag": self.line[96:97],
                "inactive_date": self.line[97:105],
            }
        )
        return record


class SegmentB2Parser(BaseSegmentBHParser):
    def __init__(self, line: str, offset: int = 0):
        super().__init__(line)
        self.offset = offset
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        record = self.unpack_common_fields()
        record.update(
            {
                "city1_curr_pop": self.line[25 + self.offset : 34 + self.offset],
                "city1_code_ucr": self.line[34 + self.offset : 37 + self.offset],
                "city1_msa_code": self.line[37 + self.offset : 40 + self.offset],
                "city1_last_pop": self.line[40 + self.offset : 49 + self.offset],
                "city2_curr_pop": self.line[49 + self.offset : 58 + self.offset],
                "city2_code_ucr": self.line[58 + self.offset : 61 + self.offset],
                "city2_msa_code": self.line[61 + self.offset : 64 + self.offset],
                "city2_last_pop": self.line[64 + self.offset : 73 + self.offset],
                "city3_curr_pop": self.line[73 + self.offset : 82 + self.offset],
                "city3_code_ucr": self.line[82 + self.offset : 85 + self.offset],
                "city3_msa_code": self.line[85 + self.offset : 88 + self.offset],
                "city3_last_pop": self.line[88 + self.offset : 97 + self.offset],
                "city4_curr_pop": self.line[97 + self.offset : 106 + self.offset],
                "city4_code_ucr": self.line[106 + self.offset : 109 + self.offset],
                "city4_msa_code": self.line[109 + self.offset : 112 + self.offset],
                "city4_last_pop": self.line[112 + self.offset : 121 + self.offset],
            }
        )
        return record


class SegmentB3Parser(BaseSegmentBHParser):
    def __init__(self, line: str, offset: int = 0):
        super().__init__(line)
        self.offset = offset
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        record = self.unpack_common_fields()
        record.update(
            {
                "city5_curr_pop": self.line[25 + self.offset : 34 + self.offset],
                "city5_code_ucr": self.line[34 + self.offset : 37 + self.offset],
                "city5_msa_code": self.line[37 + self.offset : 40 + self.offset],
                "city5_last_pop": self.line[40 + self.offset : 49 + self.offset],
                "ind_1_6_12": self.line[49 + self.offset : 51 + self.offset],
                "mo_reported": self.line[51 + self.offset : 53 + self.offset],
                "master_year": self.line[53 + self.offset : 57 + self.offset],
                "jan_zero_rpt": self.line[57 + self.offset : 58 + self.offset],
                "jan_a_or_b": self.line[58 + self.offset : 59 + self.offset],
                "jan_window": self.line[59 + self.offset : 60 + self.offset],
                "feb_zero_rpt": self.line[60 + self.offset : 61 + self.offset],
                "feb_a_or_b": self.line[61 + self.offset : 62 + self.offset],
                "feb_window": self.line[62 + self.offset : 63 + self.offset],
                "mar_zero_rpt": self.line[63 + self.offset : 64 + self.offset],
                "mar_a_or_b": self.line[64 + self.offset : 65 + self.offset],
                "mar_window": self.line[65 + self.offset : 66 + self.offset],
                "apr_zero_rpt": self.line[66 + self.offset : 67 + self.offset],
                "apr_a_or_b": self.line[67 + self.offset : 68 + self.offset],
                "apr_window": self.line[68 + self.offset : 69 + self.offset],
                "may_zero_rpt": self.line[69 + self.offset : 70 + self.offset],
                "may_a_or_b": self.line[70 + self.offset : 71 + self.offset],
                "may_window": self.line[71 + self.offset : 72 + self.offset],
                "jun_zero_rpt": self.line[72 + self.offset : 73 + self.offset],
                "jun_a_or_b": self.line[73 + self.offset : 74 + self.offset],
                "jun_window": self.line[74 + self.offset : 75 + self.offset],
                "jul_zero_rpt": self.line[75 + self.offset : 76 + self.offset],
                "jul_a_or_b": self.line[76 + self.offset : 77 + self.offset],
                "jul_window": self.line[77 + self.offset : 78 + self.offset],
                "aug_zero_rpt": self.line[78 + self.offset : 79 + self.offset],
                "aug_a_or_b": self.line[79 + self.offset : 80 + self.offset],
                "aug_window": self.line[80 + self.offset : 81 + self.offset],
                "sep_zero_rpt": self.line[81 + self.offset : 82 + self.offset],
                "sep_a_or_b": self.line[82 + self.offset : 83 + self.offset],
                "sep_window": self.line[83 + self.offset : 84 + self.offset],
                "oct_zero_rpt": self.line[84 + self.offset : 85 + self.offset],
                "oct_a_or_b": self.line[85 + self.offset : 86 + self.offset],
                "oct_window": self.line[86 + self.offset : 87 + self.offset],
                "nov_zero_rpt": self.line[87 + self.offset : 88 + self.offset],
                "nov_a_or_b": self.line[88 + self.offset : 89 + self.offset],
                "nov_window": self.line[89 + self.offset : 90 + self.offset],
                "dec_zero_rpt": self.line[90 + self.offset : 91 + self.offset],
                "dec_a_or_b": self.line[91 + self.offset : 92 + self.offset],
                "dec_window": self.line[92 + self.offset : 93 + self.offset],
                "cnty1_fips": self.line[93 + self.offset : 96 + self.offset],
                "cnty2_fips": self.line[96 + self.offset : 99 + self.offset],
                "cnty3_fips": self.line[99 + self.offset : 102 + self.offset],
                "cnty4_fips": self.line[102 + self.offset : 105 + self.offset],
                "cnty5_fips": self.line[105 + self.offset : 108 + self.offset],
            }
        )
        return record


class SegmentBHParser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        b1_parser = SegmentB1Parser(self.line)
        b2_parser = SegmentB2Parser(self.line, offset=80)
        b3_parser = SegmentB3Parser(self.line, offset=176)

        record = {}
        shared_fields = ("segment", "state_code", "ori", "incident_no")
        record.update({{k}: v for k, v in b1_parser.record.items()})
        record.update({{k}: v for k, v in b2_parser.record.items() if k not in shared_fields})
        record.update({{k}: v for k, v in b3_parser.record.items() if k not in shared_fields})
        return record


class SegmentBHParserV0:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "ori_added": self.line[25:33],
            "ori_nibrs": self.line[33:41],
            "city": self.line[41:71],
            "state_abbr": self.line[71:73],
            "pop_group": self.line[73:75],
            "division": self.line[75:76],
            "region": self.line[76:77],
            "agency_ind": self.line[77:78],
            "core_city": self.line[78:79],
            "covered_by_ori": self.line[79:88],
            "fbi_field": self.line[89:92],
            "judicial_dist": self.line[92:96],
            "nibrs_flag": self.line[96:97],
            "inactive_date": self.line[97:105],
            "cnty1_curr_pop": self.line[105:114],
            "cnty1_code_ucr": self.line[114:117],
            "cnty1_msa_code": self.line[117:120],
            "cnty1_last_pop": self.line[120:129],
            "cnty2_curr_pop": self.line[129:138],
            "cnty2_code_ucr": self.line[138:141],
            "cnty2_msa_code": self.line[141:144],
            "cnty2_last_pop": self.line[144:153],
            "cnty3_curr_pop": self.line[153:162],
            "cnty3_code_ucr": self.line[163:165],
            "cnty3_msa_code": self.line[166:168],
            "cnty3_last_pop": self.line[168:177],
            "cnty4_curr_pop": self.line[177:186],
            "cnty4_code_ucr": self.line[186:189],
            "cnty4_msa_code": self.line[189:192],
            "cnty4_last_pop": self.line[192:201],
            "cnty5_curr_pop": self.line[201:210],
            "cnty5_code_ucr": self.line[210:213],
            "cnty5_msa_code": self.line[213:216],
            "cnty5_last_pop": self.line[216:225],
            "ind_1_6_12": self.line[225:227],
            "mo_reported": self.line[227:229],
            "master_year": self.line[229:233],
            "jan_zero_rpt": self.line[233:234],
            "jan_a_or_b": self.line[234:235],
            "jan_window": self.line[235:236],
            "feb_zero_rpt": self.line[236:237],
            "feb_a_or_b": self.line[237:238],
            "feb_window": self.line[238:239],
            "mar_zero_rpt": self.line[239:240],
            "mar_a_or_b": self.line[240:241],
            "mar_window": self.line[241:242],
            "apr_zero_rpt": self.line[242:243],
            "apr_a_or_b": self.line[243:244],
            "apr_window": self.line[244:245],
            "may_zero_rpt": self.line[245:246],
            "may_a_or_b": self.line[246:247],
            "may_window": self.line[247:248],
            "jun_zero_rpt": self.line[248:249],
            "jun_a_or_b": self.line[249:250],
            "jun_window": self.line[250:251],
            "jul_zero_rpt": self.line[251:252],
            "jul_a_or_b": self.line[252:253],
            "jul_window": self.line[253:254],
            "aug_zero_rpt": self.line[254:255],
            "aug_a_or_b": self.line[255:256],
            "aug_window": self.line[256:257],
            "sep_zero_rpt": self.line[257:258],
            "sep_a_or_b": self.line[258:259],
            "sep_window": self.line[259:260],
            "oct_zero_rpt": self.line[260:261],
            "oct_a_or_b": self.line[261:262],
            "oct_window": self.line[262:263],
            "nov_zero_rpt": self.line[263:264],
            "nov_a_or_b": self.line[264:265],
            "nov_window": self.line[265:266],
            "dec_zero_rpt": self.line[266:267],
            "dec_a_or_b": self.line[267:268],
            "dec_window": self.line[268:269],
            "cnty1_fips": self.line[269:272],
            "cnty2_fips": self.line[272:275],
            "cnty3_fips": self.line[275:278],
            "cnty4_fips": self.line[278:281],
            "cnty5_fips": self.line[281:284],
        }


class Segment01Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "incident_date": self.line[25:33],
            "report_date_ind": self.line[33:34],
            "incident_hour": self.line[34:36],
            "total_offenses": self.line[36:38],
            "total_victims": self.line[38:41],
            "total_offenders": self.line[41:43],
            "total_arrestees": self.line[43:45],
            "city_submission": self.line[45:49],
            "ex_cleared": self.line[49:50],
            "ex_cleared_date": self.line[50:58],
        }


class SegmentW1Parser(Segment01Parser):
    def __init__(self, line: str):
        super().__init__(line)
        self.record.update(self.unpack_w1_fields())

    def unpack_w1_fields(self) -> dict:
        return {
            "ucr_offense1": self.line[58:61],
            "ucr_offense2": self.line[61:64],
            "ucr_offense3": self.line[64:67],
            "ucr_offense4": self.line[67:70],
            "ucr_offense5": self.line[70:73],
            "ucr_offense6": self.line[73:76],
            "ucr_offense7": self.line[76:79],
            "ucr_offense8": self.line[79:82],
            "ucr_offense9": self.line[82:85],
            "ucr_offense10": self.line[85:88],
        }


class Segment02Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "incident_date": self.line[25:33],
            "ucr_offense_code": self.line[33:36],
            "attempted_or_completed": self.line[36:37],
            "offender1_suspected_of_using": self.line[37:38],
            "offender2_suspected_of_using": self.line[38:39],
            "offender3_suspected_of_using": self.line[39:40],
            "location_type": self.line[40:42],
            "premises_entered": self.line[42:44],
            "forced_entry": self.line[44:45],
            "criminal_activity_type1": self.line[45:46],
            "criminal_activity_type2": self.line[46:47],
            "criminal_activity_type3": self.line[47:48],
            "weapon_or_force_type1": self.line[48:50],
            "automatic_ind1": self.line[50:51],
            "weapon_or_force_type2": self.line[51:53],
            "automatic_ind2": self.line[53:54],
            "weapon_or_force_type3": self.line[54:56],
            "automatic_ind3": self.line[56:57],
            "bias_motivation": self.line[57:59],
        }


class Segment03Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "incident_date": self.line[25:33],
            "property_loss_type": self.line[33:34],
            "property_descr": self.line[34:36],
            "property_value": self.line[36:45],
            "date_recovered": self.line[45:53],
            "motor_vehicles_stolen": self.line[53:55],
            "motor_vehicles_recovered": self.line[55:57],
            "suspected_drug1": self.line[57:58],
            "suspected_drug1_qty": self.line[58:67],
            "suspected_drug1_qty_thous": self.line[67:70],
            "suspected_drug1_qty_units": self.line[70:72],
            "suspected_drug2": self.line[72:73],
            "suspected_drug2_qty": self.line[73:82],
            "suspected_drug2_qty_thous": self.line[82:85],
            "suspected_drug2_qty_units": self.line[85:87],
            "suspected_drug3": self.line[87:88],
            "suspected_drug3_qty": self.line[88:97],
            "suspected_drug3_qty_thous": self.line[97:100],
            "suspected_drug3_qty_units": self.line[100:102],
        }


class SegmentW3Parser(Segment03Parser):
    def __init__(self, line: str):
        super().__init__(line)
        self.record.update(self.unpack_w3_fields())

    def unpack_w3_fields(self) -> dict:
        return {
            "ucr_offense_code1": self.line[102:105],
            "ucr_offense_code2": self.line[105:108],
            "ucr_offense_code3": self.line[108:111],
            "ucr_offense_code4": self.line[111:114],
            "ucr_offense_code5": self.line[114:117],
            "ucr_offense_code6": self.line[117:120],
            "ucr_offense_code7": self.line[120:123],
            "ucr_offense_code8": self.line[123:126],
            "ucr_offense_code9": self.line[126:129],
            "ucr_offense_code10": self.line[129:132],
        }


class Segment04Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "incident_date": self.line[25:33],
            "victim_seq_no": self.line[33:36],
            "victim_of_ucr_offense1": self.line[36:39],
            "victim_of_ucr_offense2": self.line[39:42],
            "victim_of_ucr_offense3": self.line[42:45],
            "victim_of_ucr_offense4": self.line[45:48],
            "victim_of_ucr_offense5": self.line[48:51],
            "victim_of_ucr_offense6": self.line[51:54],
            "victim_of_ucr_offense7": self.line[54:57],
            "victim_of_ucr_offense8": self.line[57:60],
            "victim_of_ucr_offense9": self.line[60:63],
            "victim_of_ucr_offense10": self.line[63:66],
            "victim_type": self.line[66:67],
            "victim_age": self.line[68:69],
            "victim_sex": self.line[69:70],
            "victim_race": self.line[70:71],
            "victim_ethnicity": self.line[71:72],
            "victim_resident_status": self.line[72:73],
            "agg_aslt_hom_circumstances1": self.line[73:75],
            "agg_aslt_hom_circumstances2": self.line[75:77],
            "justifiable_hom_circumstances": self.line[77:78],
            "victim_injury1_type": self.line[78:79],
            "victim_injury2_type": self.line[79:80],
            "victim_injury3_type": self.line[80:81],
            "victim_injury4_type": self.line[81:82],
            "victim_injury5_type": self.line[82:83],
            "offender_no_relation1": self.line[83:85],
            "victim_offender_relation1": self.line[85:87],
            "offender_no_relation2": self.line[87:89],
            "victim_offender_relation2": self.line[89:91],
            "offender_no_relation3": self.line[91:93],
            "victim_offender_relation3": self.line[93:95],
            "offender_no_relation4": self.line[95:97],
            "victim_offender_relation4": self.line[97:99],
            "offender_no_relation5": self.line[99:101],
            "victim_offender_relation5": self.line[101:103],
            "offender_no_relation6": self.line[103:105],
            "victim_offender_relation6": self.line[105:107],
            "offender_no_relation7": self.line[107:109],
            "victim_offender_relation7": self.line[109:111],
            "offender_no_relation8": self.line[111:113],
            "victim_offender_relation8": self.line[113:115],
            "offender_no_relation9": self.line[115:117],
            "victim_offender_relation9": self.line[117:119],
            "offender_no_relation10": self.line[119:121],
            "victim_offender_relation10": self.line[121:123],
        }


class Segment05Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "incident_date": self.line[25:33],
            "offender_seq_no": self.line[33:35],
            "offender_age": self.line[36:37],
            "offender_sex": self.line[37:38],
            "offender_race": self.line[38:39],
        }


class Segment06Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "incident_date": self.line[25:33],
            "arrestee_seq_no": self.line[33:35],
            "arrest_transaction_no": self.line[35:47],
            "arrest_date": self.line[47:55],
            "arrest_type": self.line[55:56],
            "multi_arrestee_seg_ind": self.line[56:57],
            "ucr_arrest_offense_code": self.line[57:60],
            "arrestee_weapon1": self.line[60:62],
            "arrestee_weapon1_automatic": self.line[62:63],
            "arrestee_weapon2": self.line[63:65],
            "arrestee_weapon2_automatic": self.line[65:66],
            "arrestee_age": self.line[66:68],
            "arrestee_sex": self.line[68:69],
            "arrestee_race": self.line[69:70],
            "arrestee_ethnicity": self.line[70:71],
            "arrestee_residency": self.line[71:72],
            "arrestee_under_18_disp": self.line[72:73],
        }


class SegmentW6Parser(Segment06Parser):
    def __init__(self, line: str):
        super().__init__(line)
        self.record.update(self.unpack_w6_fields())

    def unpack_w6_fields(self) -> dict:
        return {
            "window_clearance_flag": self.line[73:74],
            "ucr_offense1": self.line[74:77],
            "ucr_offense2": self.line[77:80],
            "ucr_offense3": self.line[80:83],
            "ucr_offense4": self.line[83:86],
            "ucr_offense5": self.line[86:89],
            "ucr_offense6": self.line[89:92],
            "ucr_offense7": self.line[92:95],
            "ucr_offense8": self.line[95:98],
            "ucr_offense9": self.line[98:101],
            "ucr_offense10": self.line[101:104],
        }


class Segment07Parser:
    def __init__(self, line: str):
        self.line = line
        self.record = self.unpack_record()

    def unpack_record(self) -> dict:
        return {
            "segment": self.line[0:2],
            "state_code": self.line[2:4],
            "ori": self.line[4:13],
            "incident_no": self.line[13:25],
            "arrest_date": self.line[25:33],
            "arrestee_seq_no": self.line[33:35],
            "city_submission": self.line[35:39],
            "arrest_type": self.line[39:40],
            "ucr_offense": self.line[40:43],
            "arrestee_weapon1": self.line[43:45],
            "arrestee_weapon1_automatic": self.line[45:46],
            "arrestee_weapon2": self.line[46:47],
            "arrestee_weapon2_automatic": self.line[47:49],
            "arrestee_age": self.line[49:51],
            "arrestee_sex": self.line[51:52],
            "arrestee_race": self.line[52:53],
            "arrestee_ethnicity": self.line[53:54],
            "arrestee_residency": self.line[54:55],
            "arrestee_under_18_disp": self.line[55:56],
        }
