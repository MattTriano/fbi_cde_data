STATE_CODES = {
    "01": "AL",
    "02": "AZ",
    "03": "AR",
    "04": "CA",
    "05": "CO",
    "06": "CT",
    "07": "DE",
    "08": "DC",
    "09": "FL",
    "10": "GA",
    "11": "ID",
    "12": "IL",
    "13": "IN",
    "14": "IA",
    "15": "KS",
    "16": "16",
    "17": "LA",
    "18": "ME",
    "19": "MD",
    "20": "MA",
    "21": "MI",
    "22": "MN",
    "23": "MS",
    "24": "MO",
    "25": "MT",
    "26": "NB",
    "27": "NV",
    "28": "NH",
    "29": "NJ",
    "30": "NM",
    "31": "NY",
    "32": "NC",
    "33": "ND",
    "34": "OH",
    "35": "OK",
    "36": "OR",
    "37": "PA",
    "38": "RI",
    "39": "SC",
    "40": "SD",
    "41": "TN",
    "42": "TX",
    "43": "UT",
    "44": "VT",
    "45": "VA",
    "46": "WA",
    "47": "WV",
    "48": "WI",
    "49": "WY",
    "50": "AK",
    "51": "HI",
    "52": "CZ",
    "53": "PR",
    "54": "AS",
    "55": "GM",
    "62": "VI",
}

POP_GROUP = {
    "0": "Possessions (PR, Guam, Canal Zone, VI, AS)",
    "1": "All cities 250K or over",
    "1A": "Cities 1M or over",
    "1B": "Cities from 500K thru 999999",
    "1C": "Cities from 250K thru 499999",
    "2": "Cities from 100K thru 249999",
    "3": "Cities from 50K thru 99999",
    "4": "Cities from 25K thru 49999",
    "5": "Cities from 10K thru 24999",
    "6": "Cities from 2500 thru 9999",
    "7": "Cities under 2500",
    "8": "Non-MSA Counties",
    "8A": "Non-MSA Counties 100K or over",
    "8B": "Non-MSA Counties from 25K thru 99999",
    "8C": "Non-MSA Counties from 10K thru 24999",
    "8D": "Non-MSA Counties under 10K",
    "9": "MSA Counties",
    "9A": "MSA Counties 100K or over",
    "9B": "MSA Counties from 25K thru 99999",
    "9C": "MSA Counties from 10K thru 24999",
    "9D": "MSA Counties under 10K",
    "9E": "MSA State Police",
}

DIVISION = {
    "0": "Possessions",
    "1": "New England",
    "2": "Middle Atlantic",
    "3": "East North Central",
    "4": "West North Central",
    "5": "South Atlantic",
    "6": "East South Central",
    "7": "West South Central",
    "8": "Mountain",
    "9": "Pacific",
}

REGION = {
    "1": "North East",
    "2": "North Central",
    "3": "South",
    "4": "West",
}

AGENCY_IND = {
    "0": "Covered by Another Agency",
    "1": "City",
    "2": "County",
    "3": "University of College",
    "4": "State Police",
}

EX_CLEARED_CODES = {
    "A": "Death of the offender",
    "B": "Prosecution declined (for other than lack of probable cause)",
    "C": "Extradition denied",
    "D": "Victim refused to cooperate",
    "E": "Juvenile/no custody",
    "N": "Not applicable",
}

UCR_OFFENSE_CODES = {
    "09A": "Murder/Nonnegligent Manslaughter",
    "09B": "Negligent Manslaughter",
    "09C": "Justifiable Homicide",
    "100": "Kidnapping/Abduction",
    "11A": "Forcible Rape",
    "11B": "Forcible Sodomy",
    "11C": "Sexual Assault With An Object",
    "11D": "Forcible Fondling (Indecent Liberties/Child Molest)",
    "120": "Robbery",
    "13A": "Aggravated Assault",
    "13B": "Simple Assault",
    "13C": "Intimidation",
    "200": "Arson",
    "210": "Extortion/Blackmail",
    "220": "Burglary/Breaking and Entering",
    "23A": "Pocket Picking",
    "23B": "Purse Snatching",
    "23C": "Shoplifting",
    "23D": "Theft From Building",
    "23E": "Theft From Coin-Operated Machine/Device",
    "23F": "Theft From Motor Vehicle",
    "23G": "Theft Of Motor Vehicle Parts/Accessories",
    "23H": "All Other Larceny",
    "240": "Motor Vehicle Theft",
    "250": "Counterfeiting/Forgery",
    "26A": "False Pretenses/Swindle/Confidence Game",
    "26B": "Credit Card/Automatic Teller Machine Fraud",
    "26C": "Impersonation",
    "26D": "Welfare Fraud",
    "26E": "Wire Fraud",
    "270": "Embezzlement",
    "280": "Stolen Property Offenses (Receiving, Selling, Etc)",
    "290": "Destruction/Damage/Vandalism of Property",
    "35A": "Drug/Narcotic Violations",
    "35B": "Drug Equipment Violations",
    "36A": "Incest - Nonforcible",
    "36B": "Statutory Rape - Nonforcible",
    "370": "Pornography/Obscene Material",
    "39A": "Betting/Wagering",
    "39B": "Operating/Promoting/Assisting Gambling",
    "39C": "Gambling Equipment Violations",
    "39D": "Sports Tampering",
    "40A": "Prostitution",
    "40B": "Assisting or Promoting Prostitution",
    "510": "Bribery",
    "520": "Weapon Law Violations",
    "90A": "Bad Checks",
    "90B": "Curfew/Loitering/Vagrancy Violation",
    "90C": "Disorderly Conduct",
    "90D": "Driving Under the Influence",
    "90E": "Drunkenness",
    "90F": "Family Offenses, Nonviolent",
    "90G": "Liquor Law Violations",
    "90H": "Peeping Tom",
    "90I": "Runaway",
    "90J": "Trespass of Real Property",
    "90Z": "All Other Offenses",
}

SUSPECTED_OF_USING_CODES = {
    "A": "Alcohol",
    "C": "Computer Equipment",
    "D": "Drugs/Narcotics",
    "N": "Not applicable",
}

LOCATION_TYPE_CODES = {
    "01": "Air/Bus/Train Terminal",
    "02": "Bank/Savings and Loan",
    "03": "Bar/Nightclub",
    "04": "Church/Synagogue/Temple",
    "05": "Commercial/Office Building",
    "06": "Construction Site",
    "07": "Convenience Store",
    "08": "Department/Discount Store",
    "09": "Drug Store/Dr's Office/Hospital",
    "10": "Field/Woods",
    "11": "Government/Public Building",
    "12": "Grocery/Supermarket",
    "13": "Highway/Road/Alley",
    "14": "Hotel/Motel/Etc",
    "15": "Jail/Prison",
    "16": "Lake/Waterway",
    "17": "Liquor Store",
    "18": "Parking Lot/Garage",
    "19": "Rental Storage Facility",
    "20": "Residence/Home",
    "21": "Restaurant",
    "22": "School/College",
    "23": "Service/Gas Station",
    "24": "Specialty Store (TV, Fur, Etc)",
    "25": "Other/Unknown",
}

CRIMINAL_ACTIVITY_TYPES = {
    "B": "Buying/Receiving",
    "C": "Cultivating/Manufacturing/Publishing",
    "D": "Distributing/Selling",
    "E": "Exploiting Children",
    "O": "Operating/Promoting/Assisting",
    "P": "Possessing/Concealing",
    "T": "Transporting/Transmitting/Importing",
    "U": "Using/Consuming",
}

BIAS_MOTIVATION_CODES = {
    "11": "White",
    "12": "Black",
    "13": "American Indian or Alaskan Native",
    "14": "Asian/Pacific Islander",
    "15": "Multi-Racial Group",
    "21": "Jewish",
    "22": "Catholic",
    "23": "Protestant",
    "24": "Islamic (Moslem)",
    "25": "Other Religion",
    "26": "Multi-Religious Group",
    "27": "Atheism/Agnosticism",
    "31": "Arab",
    "32": "Hispanic",
    "33": "Other Ethnicity/Natl. Origin",
    "41": "Male Homosexual (Gay)",
    "42": "Female Homosexual (Lesbian)",
    "43": "Homosexual (Gay and Lesbian)",
    "44": "Heterosexual",
    "45": "Bisexual",
    "88": "None",
    "99": "Unknown",
}

type_property_loss = {
    "1": "None",
    "2": "Burned",
    "3": "Counterfeited/Forged",
    "4": "Destroyed/Damaged/Vandalized",
    "5": "Recovered",
    "6": "Seized",
    "7": "Stolen/Etc. (includes bribed, defrauded, embezzled, extorted, ransomed, robbed, etc.)",
    "8": "Unknown",
}

property_description = {
    "01": "Aircraft",
    "02": "Alcohol",
    "03": "Automobiles",
    "04": "Bicycles",
    "05": "Buses",
    "06": "Clothes/Furs",
    "07": "Computer Hardware/Software",
    "08": "Consumable Goods",
    "09": "Credit/Debit Cards",
    "10": "Drugs/Narcotics",
    "11": "Drug/Narcotic Equip.",
    "12": "Farm Equipment",
    "13": "Firearms",
    "14": "Gambling Equipment",
    "15": "Heavy Construction/Industrial Equipment",
    "16": "Household Goods",
    "17": "Jewelry/Precious Metals",
    "18": "Livestock",
    "19": "Merchandise",
    "20": "Money",
    "21": "Negotiable Instruments",
    "22": "Nonnegotiable Instruments",
    "23": "Office-Type Equipment",
    "24": "Other Motor Vehicles",
    "25": "Purses/Handbags/Wallets",
    "26": "Radios/TVs/VCRs",
    "27": "Recordings-Audio/Visual",
    "28": "Recreational Vehicles",
    "29": "Structures-Single Occupancy Dwellings",
    "30": "Structures-Other Dwellings",
    "31": "Structures - Commercial/Business",
    "32": "Structures - Industrial/Manufacturing",
    "33": "Structures Public/Community",
    "34": "Structures-Storage",
    "35": "Structures-Other",
    "36": "Tools-Power/Hand",
    "37": "Trucks",
    "38": "Vehicle Parts/Accessories",
    "39": "Watercraft",
    "77": "Other",
    "88": "Pending Inventory (of Property)",
    "99": "Special Category",
}

suspected_drug_type = {
    "A": "'Crack' Cocaine",
    "B": "Cocaine (all forms except 'Crack')",
    "C": "Hashish",
    "D": "Heroin",
    "E": "Marijuana",
    "F": "Morphine",
    "G": "Opium",
    "H": "Other Narcotics: Codeine; Demerol; Dihydromorphinone or Dilaudid; Hydrocodone or Percodan; Methadone; etc.",
    "I": "LSD",
    "J": "PCP",
    "K": "Other Hallucinogens: BMDA ('White Acid'); DMT; MDA; MDMA; Mescaline or Peyote; Psilocybin; STP; etc.",
    "L": "Amphetamines/Methamphetamines",
    "M": "Other Stimulants: Adipex, Fastine, and Ionamin (Derivatives of Phentermine); Benzedrine; Didrex; Methylphenidate or Ritalin; Phenmetrazine or Preludin; Tenuate; etc.",
    "N": "Barbiturates",
    "O": "Other Depressants: Glutethimide or Doriden; Methaqualone or Quaalude; Pentazocine or Talwin; etc.",
    "P": "Other Drugs: Antidepressants (Elavil, Triavil, Tofranil, etc.); Aromatic Hydrocarbons; Propoxyphene or Darvon; Tranquilizers (Chlordiazepoxide or Librium, Diazepam or Valium, etc.); etc.",
    "U": "Unknown Type Drug",
    "X": "Over 3 Drug Types",
}

type_of_victim = {
    "I": "Individual",
    "B": "Business",
    "F": "Financial Institution",
    "G": "Government",
    "R": "Religious Organization",
    "S": "Society/Public",
    "O": "Other",
    "U": "Unknown",
}

age_of_victim = {
    "NN": "Under 24 Hours (neonate)",
    "NB": "1-6 Days Old",
    "BB": "7-364 Days Old",
    "00": "Unknown",
    "01-98": "Age in Years",
    "99": "Over 98 Years Old",
}


class SegmentBHParser:
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
