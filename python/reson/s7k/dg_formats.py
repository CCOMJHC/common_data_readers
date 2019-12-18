import logging
import struct
from enum import Enum


class ResonDatagrams(Enum):

    REFPOINT = 1000
    SENSOROFFSETS = 1001
    SENSOROFFSETSCALIB = 1002
    POSITION = 1003
    ATTITUDECUSTOM = 1004
    TIDE = 1005
    ALTITUDE = 1006
    MOTIONOVERGROUND = 1007
    DEPTH = 1008
    SOUNDVELOCTIYPROFILE = 1009
    CTD = 1010
    GEODESY = 1011
    ROLLPITCHHEAVE = 1012
    HEADING = 1013
    SURVEYLINE = 1014
    NAVIGATION = 1015
    ATTITUDE = 1016
    PANTILT = 1017

    SONARSETTINGS = 7000
    SONARCONFIG = 7001
    MATCHFILTER = 7002
    FIRMWARECONFIG = 7003
    BEAMGEO = 7004
    BATHYDATA = 7006
    SIDESCAN = 7007
    WATERCOLUMNGEN = 7008
    TVG = 7010
    IMAGEDATA = 7011
    PINGMOTIONDATA = 7012
    BEAMFORMEDDATA = 7018
    BUILTINTEST = 7021
    VERSION7K = 7022
    RAWDETECTDATA = 7027
    SNIPPETDATA = 7028
    VERNIERDATA = 7029
    BEAMFORMEDCOMPRESSED = 7041
    WATERCOLUMNCOMPRESSED = 7042
    BEAMDATACALIBRATED = 7048
    SIDESCANCALIBRATED = 7057
    SNIPPETBSSTRENGTH = 7058


reson_datagram_code = {
    ResonDatagrams.REFPOINT: 1000,
    ResonDatagrams.SENSOROFFSETS: 1001,
    ResonDatagrams.SENSOROFFSETSCALIB: 1002,
    ResonDatagrams.POSITION: 1003,
    ResonDatagrams.ATTITUDECUSTOM: 1004,
    ResonDatagrams.TIDE: 1005,
    ResonDatagrams.ALTITUDE: 1006,
    ResonDatagrams.MOTIONOVERGROUND: 1007,
    ResonDatagrams.DEPTH: 1008,
    ResonDatagrams.SOUNDVELOCTIYPROFILE: 1009,
    ResonDatagrams.CTD: 1010,
    ResonDatagrams.GEODESY: 1011,
    ResonDatagrams.ROLLPITCHHEAVE: 1012,
    ResonDatagrams.HEADING: 1013,
    ResonDatagrams.SURVEYLINE: 1014,
    ResonDatagrams.NAVIGATION: 1015,
    ResonDatagrams.ATTITUDE: 1016,
    ResonDatagrams.PANTILT: 1017,
    ResonDatagrams.SONARSETTINGS: 7000,
    ResonDatagrams.SONARCONFIG: 7001,
    ResonDatagrams.MATCHFILTER: 7002,
    ResonDatagrams.FIRMWARECONFIG: 7003,
    ResonDatagrams.BEAMGEO: 7004,
    ResonDatagrams.BATHYDATA: 7006,
    ResonDatagrams.SIDESCAN: 7007,
    ResonDatagrams.WATERCOLUMNGEN: 7008,
    ResonDatagrams.TVG: 7010,
    ResonDatagrams.IMAGEDATA: 7011,
    ResonDatagrams.PINGMOTIONDATA: 7012,
    ResonDatagrams.BEAMFORMEDDATA: 7018,
    ResonDatagrams.BUILTINTEST: 7021,
    ResonDatagrams.VERSION7K: 7022,
    ResonDatagrams.RAWDETECTDATA: 7027,
    ResonDatagrams.SNIPPETDATA: 7028,
    ResonDatagrams.VERNIERDATA: 7029,
    ResonDatagrams.BEAMFORMEDCOMPRESSED: 7041,
    ResonDatagrams.WATERCOLUMNCOMPRESSED: 7042,
    ResonDatagrams.BEAMDATACALIBRATED: 7048,
    ResonDatagrams.SIDESCANCALIBRATED: 7058,
    ResonDatagrams.SNIPPETBSSTRENGTH: 7058
}


class ResonData:
    num_beams_max = 512

    def __init__(self):
        self.desc = None
        self.time = None
        self.num_beams_max = ResonData.num_beams_max
        self.parse_check = None
        self.header_fmt = None
        self.header_size = None

    def parse(self, chunk):
        raise RuntimeError("Not Implemented")


class Data1003(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Position"
        self.parse_check = False
        self.header_fmt = '<If3d5B'
        self.header_size = struct.calcsize(self.header_fmt)

        self.datum = None
        self.latency = None
        self.latitude = None
        self.longitude = None
        self.datum_height = None
        self.position_flag = None
        self.qual_flag = None
        self.position_method = None
        self.num_of_satelites = None

        self.parse_check = self.parse(chunk)

    def parse(self, chunk):
        header_unpack = struct.unpack(self.header_fmt, chunk)

        if header_unpack[0] == 0:
            self.datum = 'WGS'
        else:
            self.datum = 'Reserved'

        self.latency = header_unpack[1]
        self.latitude = header_unpack[2]
        self.longitude = header_unpack[3]
        self.datum_height = header_unpack[4]
        self.position_flag = header_unpack[5]
        self.qual_flag = header_unpack[6]
        self.position_method = header_unpack[7]
        self.num_of_satelites = header_unpack[8]
        self.parse_check = True
        return self.parse_check


class Data1012(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Roll, Pitch, Heave"
        self.parse_check = False
        self.header_fmt = '<3f'
        self.header_size = struct.calcsize(self.header_fmt)

        self.roll = None
        self.pitch = None
        self.heave = None

        self.parse_check = self.parse(chunk)

    def parse(self, chunk):
        header_unpack = struct.unpack(self.header_fmt, chunk)

        self.roll = header_unpack[0]
        self.pitch = header_unpack[1]
        self.heave = header_unpack[2]
        self.parse_check = True

        return self.parse_check


class Data1013(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Heading"
        self.parse_check = False
        self.header_fmt = '<f'
        self.header_size = struct.calcsize(self.header_fmt)

        self.heading = None

        self.parse_check = self.parse(chunk)

    def parse(self, chunk):
        header_unpack = struct.unpack(self.header_fmt, chunk)

        self.heading = header_unpack[0]
        self.parse_check = True

        return self.parse_check


class Data7000(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Runtime Settings"
        self.parse_check = False
        self.header_fmt = '<QIH4f2If2H5f2I5fIf3IfI8fH'
        self.header_size = struct.calcsize(self.header_fmt)

        self.sonar_id = None
        self.ping_number = None
        self.multiping_flag = None
        self.frequency = None
        self.sample_rate = None
        self.rx_band_width = None
        self.tx_pulse_width = None
        self.tx_wave_form = None
        self.tx_envelope = None
        self.tx_envelope_param = None
        self.tx_pulse_mode = None
        self.max_pingrate = None
        self.ping_period = None
        self.range_select = None
        self.power_select = None
        self.gain_select = None
        self.control_flag = None
        self.tx_identifier = None
        self.tx_beam_steering_vertical = None
        self.tx_beam_steering_horizontal = None
        self.tx_beam_width_vertical = None
        self.tx_beam_width_horizontal = None
        self.tx_focus = None
        self.tx_shading = None
        self.tx_shading_param = None
        self.rx_identifier = None
        self.rx_shading = None
        self.rx_shading_param = None
        self.rx_flag = None
        self.rx_beam_width = None
        self.bottom_detect_range_min = None
        self.bottom_detect_range_max = None
        self.bottom_detect_depth_min = None
        self.bottom_detect_depth_max = None
        self.stabilization_roll = None
        self.stabilization_pitch = None
        self.stabilization_yaw = None
        self.absorption = None
        self.sound_velocity = None
        self.spreading = None

        self.parse(chunk)

    def parse(self, chunk):
        header_unpack = struct.unpack(self.header_fmt, chunk)

        self.sonar_id = header_unpack[0]
        self.ping_number = header_unpack[1]
        self.multiping_flag = header_unpack[2]
        self.frequency = header_unpack[3]
        self.sample_rate = header_unpack[4]
        self.rx_band_width = header_unpack[5]
        self.tx_pulse_width = header_unpack[6]
        if header_unpack[7] == 0:
            self.tx_wave_form = "CW"
        elif header_unpack[7] == 1:
            self.tx_wave_form = "LFM"
        if header_unpack[8] == 0:
            self.tx_envelope = "Tapered Rect"
        elif header_unpack[8] == 1:
            self.tx_envelope = "Tukey"
        elif header_unpack[8] == 2:
            self.tx_envelope = "Hamming"
        elif header_unpack[8] == 3:
            self.tx_envelope = "Han"
        elif header_unpack[8] == 4:
            self.tx_envelope = "Rect"
        self.tx_envelope_param = header_unpack[9]
        self.tx_pulse_mode = header_unpack[10]       # TODO: make pulse mode parser
        _ = header_unpack[11]
        self.max_pingrate = header_unpack[12]
        self.ping_period = header_unpack[13]
        self.range_select = header_unpack[14]
        self.power_select = header_unpack[15]
        self.gain_select = header_unpack[16]
        self.control_flag = header_unpack[17]   # TODO: make control flag parser
        self.tx_identifier = header_unpack[18]
        self.tx_beam_steering_vertical = header_unpack[19]
        self.tx_beam_steering_horizontal = header_unpack[20]
        self.tx_beam_width_vertical = header_unpack[21]
        self.tx_beam_width_horizontal = header_unpack[22]
        self.tx_focus = header_unpack[23]
        self.tx_shading = header_unpack[24]    # TODO: parser for shading term
        self.tx_shading_param = header_unpack[25]
        _transmit_flags = header_unpack[26]      # Currently reson does not have support for pitch and yaw stab
        self.stabilization_pitch = False
        self.stabilization_yaw = False
        self.rx_identifier = header_unpack[27]
        self.rx_shading = header_unpack[28]     # TODO: parser for rx shading term
        self.rx_shading_param = header_unpack[29]
        rcv_flags = header_unpack[30]
        rcv_flags = format(rcv_flags, '32b')
        if rcv_flags[-1] == 1:
            self.stabilization_roll = True
        else:
            self.stabilization_roll = False
        self.rx_beam_width = header_unpack[31]
        self.bottom_detect_range_min = header_unpack[32]
        self.bottom_detect_range_max = header_unpack[33]
        self.bottom_detect_depth_min = header_unpack[34]
        self.bottom_detect_depth_max = header_unpack[35]
        self.absorption = header_unpack[36]
        self.sound_velocity = header_unpack[37]
        self.spreading = header_unpack[38]
        _ = header_unpack[39]

        self.parse_check = True
        return self.parse_check


class Data7001(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7004(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Beam Geometry"
        self.parse_check = False
        self.header_fmt = '<QI'
        self.header_size = struct.calcsize(self.header_fmt)

        self.data_size = None
        self.sonar_id = None
        self.num_rx_beams = None
        self.rx_beam_number = None
        self.rx_angle_vertical = None
        self.rx_angle_horizontal = None
        self.rx_beam_width_along = None
        self.rx_beam_width_across = None
        self.parse(chunk)

    def parse(self, chunk):
        header_chunk = chunk[0:self.header_size]
        header_unpack = struct.unpack(self.header_fmt, header_chunk)
        self.sonar_id = header_unpack[0]
        self.num_rx_beams = header_unpack[1]

        data_chunk = chunk[self.header_size:]
        data_fmt = f'<%df' % (4 * self.num_rx_beams)
        data_unpack = struct.unpack(data_fmt, data_chunk)
        self.rx_beam_number = range(0, self.num_rx_beams)
        self.rx_angle_vertical = data_unpack[0:self.num_rx_beams]
        self.rx_angle_horizontal = data_unpack[self.num_rx_beams:2*self.num_rx_beams]
        self.rx_beam_width_along = data_unpack[2*self.num_rx_beams:3*self.num_rx_beams]
        self.rx_beam_width_across = data_unpack[3*self.num_rx_beams:4*self.num_rx_beams]
        logging.debug('sdf')

        self.parse_check = True
        return self.parse_check


class Data7006(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7007(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7008(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7010(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "TVG"
        self.parse_check = False
        self.header_fmt = '<QIHI8I'
        self.header_size = struct.calcsize(self.header_fmt)

        self.sonar_id = None
        self.ping_number = None
        self.multiping = None
        self.num_samples = None

        self.tvg_curve = None
        self.parse(chunk)

    def parse(self, chunk):
        header_chunk = chunk[0:self.header_size]
        header_unpack = struct.unpack(self.header_fmt, header_chunk)
        self.sonar_id = header_unpack[0]
        self.ping_number = header_unpack[1]
        self.multiping = header_unpack[2]
        self.num_samples = header_unpack[3]

        data_chunk = chunk[self.header_size:]
        fmt_tvg = '<%df' % self.num_samples
        self.tvg_curve = struct.unpack(fmt_tvg, data_chunk)

        self.parse_check = True
        return self.parse_check


class Data7017(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7018(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7027(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Raw Bathy"
        self.parse_check = False
        self.header_fmt = '<QIH2IBI3f15I'
        self.header_size = struct.calcsize(self.header_fmt)

        self.sonar_id = None
        self.ping_number = None
        self.multiping = None
        self.num_detect_ponts = None
        self.data_field_size = None
        self.detection_algorithm = None
        self.flags = None
        self.sample_rate = None
        self.tx_steering_angle = None
        self.rx_steering_angle = None
        self.beam = list()
        self.detect_point = list()
        self.rx_angle = list()
        self.beam_flag = list()
        self.quality_flag = list()
        self.uncertainty = list()
        self.signal_strength = list()
        self.min_limit = list()
        self.max_limit = list()

        self.parse_check = self.parse(chunk)

    def parse(self, chunk):
        header_chunk = chunk[0:self.header_size]
        header_unpack = struct.unpack(self.header_fmt, header_chunk)
        self.sonar_id = header_unpack[0]
        self.ping_number = header_unpack[1]
        self.multiping = header_unpack[2]
        self.num_detect_ponts = header_unpack[3]
        self.data_field_size = header_unpack[4]
        self.detection_algorithm = header_unpack[5]
        self.flags = header_unpack[6]
        self.sample_rate = header_unpack[7]
        self.tx_steering_angle = header_unpack[8]
        self.rx_steering_angle = header_unpack[9]

        data_chunk = chunk[self.header_size:]
        if self.data_field_size == 22:
            fmt_base = '<H2f2If'
        elif self.data_field_size == 26:
            fmt_base = '<H2f2I2f'
        elif self.data_field_size == 34:
            fmt_base = '<H2f2I4f'
        else:
            raise RuntimeError("Unrecognized data field size")

        offset = 0
        if self.num_detect_ponts > 0:
            for detect_point in range(self.num_detect_ponts):
                data_unpack = struct.unpack(fmt_base, data_chunk[offset:offset+self.data_field_size])
                self.beam.append(data_unpack[0])
                self.detect_point.append(data_unpack[1])
                self.rx_angle.append(data_unpack[2])
                self.beam_flag.append(data_unpack[3])
                self.quality_flag.append(data_unpack[4])
                self.uncertainty.append(data_unpack[5])
                self.signal_strength.append(data_unpack[6])
                self.min_limit.append(data_unpack[7])
                self.max_limit.append(data_unpack[8])

                offset += self.data_field_size

        self.parse_check = True
        return self.parse_check


class Data7028(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.desc = "Snippet Data"
        self.parse_check = False
        self.header_fmt = '<QI2H2BI6I'
        self.header_size = struct.calcsize(self.header_fmt)
        self.descriptor_fmt = '<H3I'
        self.descriptor_size = struct.calcsize(self.descriptor_fmt)

        self.sonar_id = None
        self.ping_number = None
        self.multiping = None
        self.num_detect_points = None
        self.error_flag = None
        self.control_flag = None
        self.flags = None

        self.beam_number = list()
        self.snippet_start_sample = list()
        self.bottom_detect_sample = list()
        self.snippet_end_sample = list()
        self.snippet_samples = list()
        self.snippet_samples_len = list()

        self.parse_check = self.parse(chunk)

    def parse(self, chunk):
        header_chunk = chunk[0:self.header_size]
        header_unpack = struct.unpack(self.header_fmt, header_chunk)

        self.sonar_id = header_unpack[0]
        self.ping_number = header_unpack[1]
        self.multiping = header_unpack[2]
        self.num_detect_points = header_unpack[3]
        self.error_flag = header_unpack[4]
        self.control_flag = header_unpack[5]
        self.flags = header_unpack[6]

        data_chunk = chunk[self.header_size:]
        offset = 0
        if self.num_detect_points > 0:
            for n in range(self.num_detect_points):
                data_unpack = struct.unpack(self.descriptor_fmt, data_chunk[offset:offset+self.descriptor_size])
                self.beam_number.append(data_unpack[0])
                self.snippet_start_sample.append(data_unpack[1])
                self.bottom_detect_sample.append(data_unpack[2])
                self.snippet_end_sample.append(data_unpack[3])
                offset += self.descriptor_size

            for beam in range(self.num_detect_points):
                num_samples = self.snippet_end_sample[beam] - self.snippet_start_sample[beam] + 1
                if (self.flags % 10) == 0:
                    snippet_fmt = '<%dH' % num_samples
                else:
                    snippet_fmt = '<%dI' % num_samples

                snippet_size = struct.calcsize(snippet_fmt)
                data = struct.unpack(snippet_fmt, data_chunk[offset:offset+snippet_size])
                self.snippet_samples.append(data)
                self.snippet_samples_len.append(num_samples)
                offset += snippet_size

        self.parse_check = True
        return self.parse_check


class Data7048(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None
        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7041(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7058(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7200(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


class Data7503(ResonData):
    def __init__(self, chunk):
        super().__init__()
        self.header = None
        self.data = None

        self.parse(chunk)

    def parse(self, chunk):
        pass


def parse(chunk: bytes, dg_type: ResonDatagrams) -> ResonData:
    if dg_type is ResonDatagrams.POSITION:
        datapacket = Data1003(chunk)

    elif dg_type is ResonDatagrams.ROLLPITCHHEAVE:
        datapacket = Data1012(chunk)

    elif dg_type is ResonDatagrams.HEADING:
        datapacket = Data1013(chunk)

    elif dg_type is ResonDatagrams.SONARSETTINGS:
        datapacket = Data7000(chunk)

    elif dg_type is ResonDatagrams.SONARCONFIG:
        datapacket = Data7001(chunk)

    elif dg_type is ResonDatagrams.BEAMGEO:
        datapacket = Data7004(chunk)

    elif dg_type is ResonDatagrams.BATHYDATA:
        datapacket = Data7006(chunk)

    elif dg_type is ResonDatagrams.SIDESCAN:
        datapacket = Data7007(chunk)

    elif dg_type is ResonDatagrams.WATERCOLUMNGEN:
        datapacket = Data7008(chunk)

    elif dg_type is ResonDatagrams.TVG:
        datapacket = Data7010(chunk)

    elif dg_type is ResonDatagrams.BEAMFORMEDDATA:
        datapacket = Data7018(chunk)

    elif dg_type is ResonDatagrams.RAWDETECTDATA:
        datapacket = Data7027(chunk)

    elif dg_type is ResonDatagrams.SNIPPETDATA:
        datapacket = Data7028(chunk)

    elif dg_type is ResonDatagrams.BEAMFORMEDCOMPRESSED:
        datapacket = Data7041(chunk)

    elif dg_type is ResonDatagrams.BEAMDATACALIBRATED:
        datapacket = Data7048(chunk)

    elif dg_type is ResonDatagrams.SNIPPETBSSTRENGTH:
        datapacket = Data7058(chunk)

    else:
        datapacket = None
        logging.error("Unsuported datagram type")

    return datapacket
