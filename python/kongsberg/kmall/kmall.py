#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A python class to read Kongsberg KMALL data format for swath mapping 
bathymetric echosounders. 
"""

import pandas as pd
import sys
import numpy as np
import struct
import datetime
import argparse
import os
from pyproj import Proj

                
class kmall():
    """ A class for reading a Kongsberg KMALL data file. """
    
    def __init__(self,filename=None):
        self.verbose = 0        
        self.filename = filename
        self.FID = None
        self.file_size = None
        self.Index = pd.DataFrame({'Time':[np.nan],
                                   'ByteOffset':[np.nan],
                                   'MessageSize':[np.nan],
                                   'MessageType':['test']})

        
        self.pingDataCheck = None
        self.navDataCheck = None
    

    def read_datagram():
        '''
        /*********************************************
        274             Datagram names
        275  *********************************************/
        276 
        277 /* I - datagrams */
        278 #define EM_DGM_I_INSTALLATION_PARAM    "#IIP"   
        279 #define EM_DGM_I_OP_RUNTIME            "#IOP"   
        280 
        281 /* S-datagrams */
        282 #define EM_DGM_S_POSITION               "#SPO"
        283 #define EM_DGM_S_KM_BINARY              "#SKM"
        284 #define EM_DGM_S_SOUND_VELOCITY_PROFILE "#SVP"
        285 #define EM_DGM_S_CLOCK                  "#SCL"
        286 #define EM_DGM_S_DEPTH                  "#SDE"
        287 #define EM_DGM_S_HEIGHT                 "#SHI"  
        288 #define EM_DGM_S_HEADING                "#SHA"
        289 
        290 /* M-datagrams */
        291 #define EM_DGM_M_RANGE_AND_DEPTH        "#MRZ"  
        292 #define EM_DGM_M_WATER_COLUMN           "#MWC"  
        
        Possible strategy for reading from a stream:
            - Read data into buffer
            - Search for all of these datagram types.
            - seek to first one minus 4 bytes for packet size. 
            - Read packet size.
            - Check to see if packet is wholly contained within buffer.
            - IF not, increase size of buffer. 
            - Read header
            - Read rest of packet.
  
    
        '''
        pass
        
    
    def read_EMdgmHeader(self):
        ''' Read datagram header'''
        dg = {}
        format_to_unpack = "I4sBBHII"
        fields = struct.unpack("I4sBBHII",self.FID.read(struct.Struct(format_to_unpack).size))
        
        dg['numBytesDgm'] = fields[0]
        dg['dgmType']     = fields[1]
        dg['dgmVersion']  = fields[2]
        dg['systemID']    = fields[3]
        dg['echoSounderID'] = fields[4]
        dg['dgtime'] = fields[5] + fields[6]/1.0E9
        dg['dgdatetime'] = datetime.datetime.utcfromtimestamp(dg['dgtime'])     

        if self.verbose > 2:
            self.print_datagram(dg) 
       
        return dg
    
    def read_EMdgmIIP(self):
        """ Read installation parameters datagram (IIP)"""
        dg = {}
        header = self.read_EMdgmHeader(self)
        for k,v in header:
            dg[k] = v
            
        format_to_unpack = "3H1B"
        fields = struct.unpack(format_to_unpack,
                               self.FID.read(struct.Struct(format_to_unpack).size))
        dg["numBytesCmnPart"] = fields[0]
        dg["info"]            = fields[1]
        dg["status"]          = fields[2]
        dg["install_txt"]     = fields[3]
        
        

        
        return dg
    
    def read_EMdgmMpartition(self):
        dg = {}
        format_to_unpack = "2H"
        fields = struct.unpack(format_to_unpack,self.FID.read(struct.Struct(format_to_unpack).size))
        dg['numOfDgms']     = fields[0]
        dg['dgmNum']        = fields[1]
        
        if self.verbose > 2:
            self.print_datagram(dg)
            
        return dg
    
    def read_EMdgmMbody(self):
        dg = {}
        format_to_unpack = "2H8B"
        fields = struct.unpack("2H8B",self.FID.read(struct.Struct(format_to_unpack).size))

        dg['numBytesCmnPart'] = fields[0]
        dg['pingCnt']         = fields[1]
        dg['rxFansPerPing']   = fields[2]
        dg['rxFanIndex']      = fields[3]
        dg['swathsPerPing']   = fields[4]
        dg['swathAlongPosition'] = fields[5]
        dg['txTransducerInd'] = fields[6]
        dg['rxTransducerInd'] = fields[7]
        dg['numRxTransducers'] = fields[8]
        dg['algorithmType']   = fields[9]  
        
        self.FID.seek(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size,1) # Skips unknown fields.

        if self.verbose > 2:
            self.print_datagram(dg)
        
        return dg
    
    def read_EMdgmIOP(self):
        """ Read Runtime Parameters IOP datagram """ 
        
        dg = self.read_EMdgmHeader(self.FID)
        format_to_unpack = "3H"
        fields = struct.unpack("3H",self.FID.read(struct.Struct(format_to_unpack).size))
        dg['numBytesCmnPart'] = fields[0]
        dg['info']            = fields[1]
        dg['status']          = fields[2]
        tmp = FID.read(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size)
        rt_text = tmp.decode('UTF-8')
        print(rt_text)
        dg['RT'] = rt_text
        
        return dg
    
    
    
    def read_EMdgmScommon(self):
        """ Read the common portion of the 'sensor' datagram. """   
        dg = {}
        format_to_unpack = "3H"
        fields = struct.unpack("3H",self.FID.read(struct.Struct(format_to_unpack).size),1)
        dg["numBytesCmnPart"]    = fields[0]
        dg["sensorSystem"]       = fields[1]
        dg["sensorStatus"]       = fields[2]
        self.FID.seek(dg["numBytesCmnPart"] - struct.Struct(format_to_unpack).size,1)
        
        return dg
    
    def read_EMdgmSKM(self):
        """ Read attitude datagrams in raw format """
        dgH = self.read_EMdgmHeader(self.FID)
        dgInfo = self.read_EMdgmSKMinfo(self.FID)
        dgSamples = self.read_EMdgmSKMsample(dgInfo)
        
        dg = {**dgH,**dgInfo,**dgSamples}
        return dg
    
    def read_EMdgmSKMsample(self,dgInfo):
        """ Read attitude and position data sammples """
        
        data = list()
        for idx in range(dgInfo['numSamplesArray']):
            if dgInfo['sensorDataContents'] == 32:
                data.append(self.read_KMdelayedHeave(dgInfo['numBytesPerSample']))
            else:
                # numBytesPerSample is contained in the attitude datagrams 
                # and does not need to be passed here, unlike delayed heave
                # which does not. 
                data.append(self.read_KMbinary())
                
        # Convert list of dictionaries to disctionary of lists.
        dg = self.listofdicts2dictoflists(data)
                        
        return dg
    
    def read_EMdgmSKMinfo(self):
        """ Read attitude metadata datagram. """
        dg = {}
        format_to_unpack = "H2B4H"
        fields = struct.unpack("H2B4H",self.FID.read(struct.Struct(format_to_unpack).size))
        dg['numBytesInfoPart']    = fields[0]
        dg['sensorSystem']        = fields[1]
        dg['sensorStatus']        = fields[2]
        '''
        Sensor Status:
            Bit: 0      0 Data OK, 1 Data OK and Sensor is active
            Bit: 1      0
            Bit: 2      0 Data OK, 1 Data Reduced Performance
            Bit: 3      0
            Bit: 4      0 Data OK, 1 Invalid Data
            Bit: 5      0
            Bit: 6      0 Velocity from Sensor, 1 Velocity from PU
        '''
        
        dg['sensorInputFormat']   = fields[3]
        '''
        Input Format:
            1: KM Binary Sensor Format
            2: EM 3000 data
            3: Sagem
            4: Seapath binary 11
            5: Seapath binary 23
            6: Seapath binary 26
            7: POS/MV Group 102/103
            9: Coda Octopus MCOM
        '''
        dg['numSamplesArray']     = fields[4]
        dg['numBytesPerSample']   = fields[5]
        dg['sensorDataContents']  = fields[6]
        '''
        sensorDataContents:
            Indicates what data is avaiable in the given sensor format
            Bit:     Sensor Data
            0        Horizontal posistion and velocity
            1        Roll and pitch
            2        Heading
            3        Heave and vertical velocity
            4        Acceleration
            5        Error fields
            6        Delayed Heave
        '''
        self.FID.seek(dg['numBytesInfoPart']-struct.Struct(format_to_unpack).size,1)
        
        if self.verbose > 2:
            self.print_datagram(dg)
            
        return dg
    
    def read_KMdelayedHeave(self,bytesPerSample):
        """ Read the delayed heave samples datagram. """
        dg = {}
        format_to_unpack = "2If"
        fields = struct.unpack("format_to_unpack",self.FID.read(struct.Struct(format_to_unpack).size))
        dg["time_sec"] = fields[0]
        dg["time_nanosec"] = fields[1]
        dg["delayedHeave_m"] = fields[2]
        
        self.FID.seek(bytesPerSample - struct.Struct(format_to_unpack).size,1)

        if self.verbose > 2:
            self.print_datagram(dg)
            
        return dg
    
    def read_KMbinary(self):
        """ Read the attitude sample datagram. """
        dg = {}
        format_to_unpack = "4B2H3I"
        fields = struct.unpack(format_to_unpack,self.FID.read(struct.Struct(format_to_unpack).size))
        dg["dgmType"] = fields[0] + fields[1] + fields[2] + fields[3]
        dg["numBytesDgm"]    = fields[4]
        dg["dgmVersion"]     = fields[5]
        dg["time_sec"]       = fields[6]
        dg["time_nanosec"]   = fields[7]
        dg["datetime"] = datetime.datetime.utcfromtimestamp(dg['time_sec'] + dg['time_nanosec']/1.0E9)   
        dg["status"]         = fields[8]
        fields = struct.unpack("2d",FID.read(16))
        dg["latitude_deg"]   = fields[0]
        dg["longitude_deg"]  = fields[1]
        fields = struct.unpack("21f",FID.read(21*4))
        dg["ellipsoidHeight_m"] = fields[0]
        dg["roll_deg"]       = fields[1]
        dg["pitch_deg"]      = fields[2]
        dg["heading_deg"]    = fields[3]
        dg["heave_m"]        = fields[4]
        dg["rollRate"]       = fields[5]
        dg["pitchRate"]      = fields[6]
        dg["yawRate"]        = fields[7]
        dg["velNorth"]       = fields[8]
        dg["velEast"]        = fields[9]
        dg["velDown"]        = fields[10]
        dg["latitudeError_m"] = fields[11]
        dg["longitudeError_m"] = fields[12]
        dg["ellipsoidalHeightError_m"] = fields[13]
        dg["rollError_deg"]  = fields[14]
        dg["pitchError_deg"] = fields[15]
        dg["headingError_deg"] = fields[16]
        dg["heaveError+m"]   = fields[17]
        dg["northAcceleration"] = fields[18]
        dg["eastAcceleration"] = fields[19]
        dg["downAcceleration"] = fields[20]
        
        self.FID.seek(dg['numBytesDgm'] - struct.Struct(format_to_unpack).size,1)

        if self.verbose > 2:
            self.print_datagram(dg)
        return dg
    
    
    def read_EMdgmSCL(self):
        """ Read data from an external sensor """
        dg = self.read_EMdgmHeader()
        return dg

    def read_EMdgmMRZ_pinginfo(self):
        """ Read MRZ ping info datagram"""
        
        dg = {}
        format_to_unpack = "2Hf6BH11f2h2BHI3f2Hf2H6f4B2df"
        fields = struct.unpack(format_to_unpack,
                               self.FID.read(struct.Struct(format_to_unpack).size))

        dg["numBytesInfoData"]           = fields[0]
        dg["padding0"]                   = fields[1]
        dg["pingRate_Hz"]                = fields[2]
        dg["beamSpacing"]                = fields[3]
        dg["depthMode"]                  = fields[4]
        dg["subDepthMode"]               = fields[5]
        dg["distanceBtwSwath"]           = fields[6]
        dg["detectionMode"]              = fields[7]
        dg["pulseForm"]                  = fields[8]
        dg["padding1"]                   = fields[9]
        dg["frequencyMode_Hz"]           = fields[10]
        dg["freqRangeLowLim_Hz"]         = fields[11]
        dg["freqRangeHighLim_Hz"]        = fields[12]
        dg["maxTotalTxPulseLength_sec"]  = fields[13]
        dg["maxEffTxPulseLength_sec"]    = fields[14]
        dg["maxEffTxBandWidth_Hz"]       = fields[15]
        dg["absCoeff_dBPerkm"]           = fields[16]
        dg["portSectorEdge_deg"]         = fields[17]
        dg["starbSectorEdge_deg"]        = fields[18]
        dg["portMeanCov_deg"]            = fields[19]
        dg["stbdMeanCov_deg"]            = fields[20]
        dg["portMeanCov_m"]              = fields[21]
        dg["starbMeanCov_m"]             = fields[22]
        dg["modeAndStabilisation"]       = fields[23]
        dg["runtimeFilter1"]             = fields[24]
        dg["runtimeFilter2"]             = fields[25]
        dg["pipeTrackingStatus"]         = fields[26]
        dg["transmitArraySizeUsed_deg"]  = fields[27]
        dg["receiveArraySizeUsed_deg"]   = fields[28]
        dg["transmitPower_dB"]           = fields[29]
        dg["SLrampUpTimeRemaining"]      = fields[30]
        dg["padding2"]                   = fields[31]
        dg["yawAngle_deg"]               = fields[32]
        # Tx Sector block
        dg["numTxSectors"]               = fields[33]
        dg["numBytesPerTxSector"]        = fields[34]
        # Info at time of midpoint of first tx pulse
        dg["headingVessel_deg"]          = fields[35]
        dg["soundSpeedAtTxDepth_mPerSec"] = fields[36]
        dg["txTransducerDepth_m"]        = fields[37]
        dg["z_waterLevelReRefPoint_m"]   = fields[38]
        dg["x_kmallToall_m"]             = fields[39]
        dg["y_kmallToall_m"]             = fields[40]
        dg["latLongInfo"]                = fields[41]
        dg["posSensorStatus"]            = fields[42]
        dg["attitudeSensorStatus"]       = fields[43]
        dg["padding3"]                   = fields[44]
        dg["latitude_deg"]               = fields[45]
        dg["longitude_deg"]               = fields[46]
        dg["ellipsoidHeightReRefPoint_m"] = fields[47]
       
        self.FID.seek(dg["numBytesInfoData"] - struct.Struct(format_to_unpack).size,1)
 
        if self.verbose > 2:
            self.print_datagram(dg)
            
        return dg
    
    def read_EMdgmMRZ_txSectorInfo(self):
        """ Read MRZz Tx Sector Info. """

        dg = {}
        format_to_unpack = "4B7f2BH"
        fields = struct.unpack(format_to_unpack,
                               self.FID.read(struct.Struct(format_to_unpack).size))
        
        dg["txSectorNumb"]          = fields[0]
        dg["txArrNumber"]           = fields[1]
        dg["txSubArray"]            = fields[2]
        dg["padding0"]              = fields[3]
        dg["sectorTransmitDelay_sec"] = fields[4]
        dg["tiltAngleReTx_deg"]     = fields[5]
        dg["txNominalSourceLevel_dB"] = fields[6]
        dg["txFocusRange_m"]        = fields[7]
        dg["centreFreq_Hz"]         = fields[8]
        dg["signalBandWidth_Hz"]    = fields[9]
        dg["totalSignalLength_sec"] = fields[10]
        dg["pulseShading"]          = fields[11]
        dg["signalWaveForm"]        = fields[12]
        dg["padding1"]              = fields[13] 
        
        # There's no fields for the number of bytes in this record. Odd. 
        # FID.seek(dg["numBytesInfoData"] - struct.Struct(format_to_unpack).size,1)
        if self.verbose > 2:
            self.print_datagram(dg)
        return dg
    
    def read_EMdgmMRZ_rxInfo(self):
        """ A method to read the MRZ Rx Info record."""
        
        dg = {}
        format_to_unpack = "4H4f4H"
        fields = struct.unpack(format_to_unpack,
                               self.FID.read(struct.Struct(format_to_unpack).size))
        
        dg["numBytesRxInfo"]        = fields[0]
        dg["numSoundingsMaxMain"]   = fields[1]
        dg["numSoundingsValidMain"] = fields[2]
        dg["numBytesPerSounding"]   = fields[3]
        dg["WCSapleRate"]           = fields[4]
        dg["seabedImageSampleRate"] = fields[5]
        dg["BSnormal_dB"]           = fields[6]
        dg["BSoblique_dB"]          = fields[7]
        dg["extraDetectionAlarmFlag"] = fields[8]
        dg["numExtraDetections"]    = fields[9]
        dg["numExtraDetectionClasses"] = fields[10]
        dg["numBytesPerClass"]      = fields[11]
        
        self.FID.seek(dg["numBytesRxInfo"] - struct.Struct(format_to_unpack).size,1)
        
        if self.verbose > 2:
            self.print_datagram(dg)
        return dg
    
    def read_EMdgmMRZ_extraDetClassInfo(self):
        
        dg = {}
        format_to_unpack = "HbB"
        fields = struct.unpack(format_to_unpack,
                               self.FID.read(struct.Struct(format_to_unpack).size))
        dg["numExtraDetinClass"] = fields[0]
        dg["padding"]            = fields[1]
        dg["alarmFlag"]          = fields[2]
        
        if self.verbose > 2:
            self.print_datagram(dg)
        return dg
    
    def read_EMdgmMRZ_sounding(self):
        
        dg = {}
        format_to_unpack = "H8BH6f2H18f4H"
        fields = struct.unpack(format_to_unpack,
                               self.FID.read(struct.Struct(format_to_unpack).size))
        
        dg["soundingIndex"]        = fields[0]
        dg["txSectorNumb"]         = fields[1]
        dg["detectionType"]        = fields[2]
        dg["detectionMethod"]      = fields[3]
        dg["rejectionInfo1"]       = fields[4]
        dg["rejectionInfo2"]       = fields[5]
        dg["postProcessingInfo"]   = fields[6]
        dg["detectionClass"]       = fields[7]
        dg["detectionConfidenceLevel"]   = fields[8]
        dg["padding"]              = fields[9]
        dg["rangeFactor"]          = fields[10]
        dg["qualityFactor"]        = fields[11]
        dg["detectionUncertaintyVer_m"] = fields[12]
        dg["detectionUncertaintyHor_m"] = fields[13]
        dg["detectionWindowLength_m"]   = fields[14]
        dg["echo_Length_sec"]           = fields[15]
        dg["WCBeamNumb"]                = fields[16]
        dg["WCrange_samples"]           = fields[17]
        dg["WCNomBeamAngleAcross_deg"]  = fields[18]
        
        dg["meanAbsCoeff_dbPerkm"]      = fields[19]
        dg["reflectivity1_dB"]          = fields[20]
        dg["reflectivity2_dB"]          = fields[21]
        dg["receiverSensitivityApplied_dB"] = fields[22]
        dg["sourceLevelApplied_dB"]     = fields[23]
        dg["BScalibration_dB"]          = fields[24]
        dg["TVG_dB"]                    = fields[25]
        
        dg["beamAngleReRx_deg"]        = fields[26]
        dg["beamAngleCorrection_deg"]  = fields[27]
        dg["twoWayTravelTime_sec"]     = fields[28]
        dg["twoWayTravelTimeCorrection_sec"]  = fields[29]
        dg["deltaLatitude_deg"]        = fields[30]
        dg["deltaLongitude_deg"]       = fields[31]
        dg["z_reRefPoint_m"]           = fields[32]
        dg["y_reRefPoint_m"]           = fields[33]
        dg["x_reRefPoint_m"]           = fields[34]
        dg["beamIncAngleAdj_deg"]      = fields[35]
        dg["realTimeCleanInfo"]        = fields[36]
        
        dg["SlstartRange_samples"]     = fields[37]
        dg["SlcenterSample"]           = fields[38]
        dg["SlnumSamples"]             = fields[39]
        
        if self.verbose > 2:
            self.print_datagram(dg)
            
        return dg
        

    def read_EMdgmMRZ(self):
        ''' A method to read a full MRZ datagram.'''
        
        start = self.FID.tell()
        
        dg = {}
        dg["header"] = self.read_EMdgmHeader()
        dg["Mpart"]  = self.read_EMdgmMpartition()
        dg["Mbody"]  = self.read_EMdgmMbody()
        dg["pinginfo"] = self.read_EMdgmMRZ_pinginfo()

        # Read TX sector info for each sector
        txSectorinfo = []
        for sector in range(dg["pinginfo"]["numTxSectors"]):
            txSectorinfo.append(self.read_EMdgmMRZ_txSectorInfo())
        dg["txSectorinfo"] = self.listofdicts2dictoflists(txSectorinfo)
    
       # Read Rxinfo    
        dg["rxinfo"] = self.read_EMdgmMRZ_rxInfo()

        # Read extra detect metadata if they exist.
        extraDetClassInfo = []
        for detclass in range(dg["rxinfo"]["numExtraDetectionClasses"]):
            extraDetClassInfo.append(self.read_EMdgmMRZ_extraDetClassInfo())
        dg["extraDetClassInfo"] = self.listofdicts2dictoflists(extraDetClassInfo)
        
        # Read the sounding data. 
        soundings = []
        Nseabedimage_samples = 0
        for sounding in range(dg["rxinfo"]["numExtraDetections"] + 
                              dg["rxinfo"]["numSoundingsMaxMain"]):
            soundings.append(self.read_EMdgmMRZ_sounding())
            Nseabedimage_samples += soundings[sounding]["SlnumSamples"]
        dg["soundings"] = self.listofdicts2dictoflists(soundings)

        # Read the seabed imagery. 
        format_to_unpack = str(Nseabedimage_samples) + "H"
        dg["Slsample_desidB"] = struct.unpack(format_to_unpack,
                                              self.FID.read(struct.Struct(format_to_unpack).size))
        
        # Seek to end of the packet. 
        self.FID.seek(start + dg["header"]["numBytesDgm"],0)
        
        return dg
 
    ###########################################################
    # Utilities
    ###########################################################

    def OpenFiletoRead(self,inputfilename=None):
        """ Open a KMALL data file for reading."""
        if self.filename is None:
            if inputfilename is None:
                print("No file name specified")
                sys.exit(1)
            else:
                filetoopen = inputfilename
        else:
            filetoopen = self.filename

        if self.verbose >= 1:
            print("Opening: %s" % filetoopen)
            
        self.FID = open(filetoopen,"rb")

    def closeFile(self):
        """ Close a file."""
        if self.FID is not None:
            self.FID.close()

    def print_datagram(self,dg):
        """ A utility function to print the fields of a parsed datagram. """
        print("\n")
        for k,v in dg.items():
            print("%s:\t\t\t%s" % (k,str(v)))
    
    def index_file(self):
        """ Index a KMALL file - message type, time, size, byte offset. """
        
        if self.FID is None:
            self.OpenFiletoRead()
        
        # Get size of the file.
        self.FID.seek(0, 2)
        self.file_size = self.FID.tell()
        self.FID.seek(0,0)
        
        if (self.verbose == 1):	
            print("Filesize: %d" % self.file_size)
        
        self.msgoffset = []
        self.msgsize = []
        self.msgtime = []
        self.msgtype = []
        self.pktcnt = 0
        while self.FID.tell() < self.file_size:
            
            try:
                # Get the byte offset.
                self.msgoffset.append(self.FID.tell())
            
                # Read the first four bytes to get the datagram size.
                msgsize = struct.unpack("I",self.FID.read(4))
                self.msgsize.append(msgsize[0])
            
                # Read the datagram.
                msg_buffer = self.FID.read(int(self.msgsize[self.pktcnt])-4)
            except:
                print("Error indexing file: %s" % self.filename)
                self.msgoffset = self.msgoffset[:-1]
                self.msgsize = self.msgsize[:-1]
                continue
            
            # Interpret the header.
            header_without_length = struct.Struct('ccccBBHII')
            
            (dgm_type0, dgm_type1, dgm_type2, dgm_type3, dgm_version,
             sysid,emid,
             sec,
             nsec) = header_without_length.unpack_from(msg_buffer,0)
            
            dgm_type = dgm_type0 + dgm_type1 + dgm_type2 + dgm_type3
		
            self.msgtype.append(str(dgm_type))
            # Decode time
            #osec = sec
            #osec *= 1E9
            #osec += nsec
            #lisec = nanosec
            #lisec /= 1E6
            
            # Captue the datagram header timestamp.
            self.msgtime.append(sec + nsec / 1.0E9)

            if self.verbose:                
                print("MSG_TYPE: %s,\tOFFSET:%0.0f,\tSIZE: %0.0f,\tTIME: %0.3f" % 
                      (dgm_type, 
                       self.msgoffset[self.pktcnt], 
                       self.msgsize[self.pktcnt],
                       self.msgtime[self.pktcnt]))
        
            self.pktcnt += 1
            
        
        self.msgoffset = np.array(self.msgoffset)
        self.msgsize = np.array(self.msgsize)
        self.msgtime = np.array(self.msgtime)
        
        self.Index = pd.DataFrame({'Time':self.msgtime,
                            'ByteOffset':self.msgoffset,
                            'MessageSize':self.msgsize,
                            'MessageType':self.msgtype})
        self.Index.set_index('Time',inplace=True)
        self.Index['MessageType'] = self.Index.MessageType.astype('category')
        if self.verbose >= 2:
            print(self.Index)  
    
    def extract_nav(self):
        pass
    
    def extract_attitude(self):
        ''' Extract all raw attitude data from data file into self.att'''
        
        if self.Index is None:
            self.index_file()
            
        if self.FID is None:
            self.OpenFiletoRead()
        
        # Get offsets for 'SKM' attitude datagrams. 
        SKMOffsets = [x for x,y in zip(self.msgoffset,self.msgtype) if y == "b'#SKM'"]
        
        dg = list()
        for offset in SKMOffsets:
            self.FID.seek(offset,0)
            dg.append(self.read_EMdgmSKM())
            
        # Convert list of dictionaries to disctionary of lists.
        self.att = self.listofdicts2dictoflists(dg)
        #for k,v in dg[0].items():
        #    self.att[k] = [x for sublist in dg for x in sublist[k]]
   
     
        self.FID.seek(0,0)
        return 

    def listofdicts2dictoflists(self,listofdicts):
        """ A utility function to convert a list of dicts to a dict of lists."""        
        dg = {}

        # This is done in two steps, handling both dictionary items that are 
        # lists and scalars separately. As long as no item combines both lists 
        # and scalars the method works. 
        #
        # There is some mechanism to handle this in a single list 
        # comprehension statement, checking for types on the fly, but I cannot
        # find any syntax that returns the proper result. 
        if len(listofdicts) == 0:
            return None
        
        for k,v in listofdicts[0].items():  
            dg[k] = [item for dictitem in listofdicts if isinstance(dictitem[k],list) for item in dictitem[k]]        
            scalartmp = [dictitem[k] for dictitem in listofdicts if not isinstance(dictitem[k],list)]
            if len(dg[k]) == 0: 
                dg[k] = scalartmp 

        return dg
    
    def extract_xyz(self):
        pass
    
    def check_ping_count(self):
        """ A method to check to see tha all required MRZ datagrams exist """
        
        if self.Index is None:
            self.index_file()
            
        if self.FID is None:
            self.OpenFiletoRead()
        
        #M = map( lambda x: x=="b'#MRZ'", self.msgtype)
        #MRZOffsets = self.msgoffset[list(M)]
        
        # Get the file byte count offset for each MRZ datagram.
        MRZOffsets = [x for x,y in zip(self.msgoffset,self.msgtype) if y == "b'#MRZ'"]
        self.pingcnt = []
        self.rxFans = []      
        self.rxFanIndex = []
        
        # Skip through the file capturing the ping count information:
        #  The ping count values
        #  The number of receive fans specified for each ping
        #  The receive fan index for each received MRZ record. 
        #
        # Notes:    A ping can span more than 1 MRZ datagrams. This happens when 
        #           1 MRZ datagram exists for each receive "fan"
        #           In dual swath mode, at least two receive fans are generated.
        #           The ping counter will not change for the second MRZ packet.

        for offset in MRZOffsets:
            self.FID.seek(offset,0)
            dg = self.read_EMdgmHeader()
            dg = self.read_EMdgmMpartition()
            dg = self.read_EMdgmMbody()
            self.pingcnt.append(dg['pingCnt'])
            self.rxFans.append(dg['rxFansPerPing'])
            self.rxFanIndex.append(dg['rxFanIndex'])
        
        self.pingcnt = np.array(self.pingcnt)
        self.rxFans = np.array(self.rxFans)
        self.rxFanIndex = np.array(self.rxFanIndex)

        # Things to check:
        # Is the total sum of rxFans equal to the number of MRZ packets?
        # Are the unique ping counter values sequential?
        # The number of muliple ping counter values has to be larger than the 
        # number of rx fans and packets. 
        
        # Sorting by ping count and then calculating the difference in 
        # successive values allows one to check to see that at least one  
        # packet exists for each ping (which may have more than one). 
        if len(self.pingcnt) > 0:
            PingCounterRange = max(self.pingcnt)-min(self.pingcnt) 
            dpu = np.diff(np.sort(np.unique(self.pingcnt)))
            NpingsMissed = sum((dpu[dpu>1]-1))
            NpingsSeen = len(np.unique(self.pingcnt))
            #MaxDiscontinuity = max(abs(dpu))

            if self.verbose > 1:
                print("File: %s\n\tPing Counter Range: %d:%d N=%d" % 
                      (self.filename, min(self.pingcnt),max(self.pingcnt),PingCounterRange))
                print("\tNumbr of pings missing: %d of %d" % (NpingsMissed, NpingsMissed+NpingsSeen))
            
        else:
            PingCounterRange = 0
            NpingsSeen = 0
            NpingsMissed = 0
            if self.verbose > 1:
                print("No pings in file.")

        #print("\tNumbr of pings seen: %d" % NpingsSeen)            
        #print('File: %s\n\tNumber of missed full pings: %d of %d' % 
        #      (self.filename, PingCounterRange - NpingsSeen, PingCounterRange ))
        

        #dp = np.diff(self.pingcnt)
        # FirstPingInSeries = np.array([x==0 for x in dp]) 
        HaveAllMRZ = True
        MissingMRZCount = 0
        # Go through every "ping" these may span multple packets...        
        for idx in range(len(self.pingcnt)):
            # Side note: This method is going to produce an warning multiple
            # times for each ping series that fails the test. Sloppy.

            # Capture how many rx fans there should be for this ping.
            N_RxFansforSeries = self.rxFans[idx]
            # Get the rxFan indices associated with this ping record.
            PingsInThisSeriesMask = np.array([x==self.pingcnt[idx] for x in self.pingcnt])
            rxFanIndicesforThisSeries = self.rxFanIndex[PingsInThisSeriesMask]
            
            # Check to see that number of records equals the total.
            if len(rxFanIndicesforThisSeries) != N_RxFansforSeries:
                if HaveAllMRZ:
                    if self.verbose > 1:
                        print("\tDetected missing MRZ records!")

                if self.verbose > 1:     
                    print('\tNot enough rxFan (MRZ) records for ping: %d: Indices %s of [0:%d] found' %
                          (self.pingcnt[idx],
                           ",".join(str(x) for x in rxFanIndicesforThisSeries),
                           N_RxFansforSeries-1))
                HaveAllMRZ = False
                MissingMRZCount = MissingMRZCount+1
        
        
        # Shamelessly creating a data frame just to get a pretty table.
        res = pd.DataFrame([["File","NpingsTotal","Pings Missed","MissingMRZRecords"],
                            [ self.filename,NpingsMissed+NpingsSeen,NpingsMissed,MissingMRZCount]])
        print(res.to_string(index=False, header=False))
        
                
        if HaveAllMRZ:
            if self.verbose > 1:
                print("\tNumber of MRZ records equals number required for each ping.")
            
                        
        return (self.filename,NpingsMissed+NpingsSeen,NpingsMissed,MissingMRZCount)
        
    
    def report_packet_types(self):
        """ A method to report datagram packet count and size in a file. """
        
        if self.Index is None:
            self.index_file()
            
        # Get a list of packet types seen. 
        types = list(set(self.msgtype))

        pktcount = {}
        pktSize = {}
        pktMinSize = {}
        pktMaxSize = {}
        # Calculate some stats. 
        for type in types:
            M = np.array(list(map( lambda x: x==type, self.msgtype)))
            pktcount[type]= sum(M)
            pktSize[type] = sum(self.msgsize[M])
            pktMinSize[type] = min(self.msgsize[M])
            pktMaxSize[type] = max(self.msgsize[M])
            
        
        #print(self.Index.groupby("MessageType").describe().reset_index())
        msg_type_group = self.Index.groupby("MessageType")
        summary = {"Count":     msg_type_group["MessageType"].count(),
                   "Size:":      msg_type_group["MessageSize"].sum(),
                   "Min Size":  msg_type_group["MessageSize"].min(),    
                   "Max Size":  msg_type_group["MessageSize"].max()}
        IndexSummary = pd.DataFrame(summary)


        print(IndexSummary)
    

if __name__ == '__main__':
    
    # Handle input arguments
    parser = argparse.ArgumentParser(description="A python script (and class)" 
                                     "for parsing Kongsberg KMALL data files.")
    parser.add_argument('-f',action='store',dest='kmall_filename',
                        help="The path and filename to parse.")
    parser.add_argument('-d', action='store',dest='kmall_directory',
                        help="A directory containing kmall data files to parse.")
    parser.add_argument('-V', action='store_true',dest='verify',
                        default=False, help="Perform series of checks to verify the kmall file.")
    parser.add_argument('-v',action='count',dest='verbose',default=0,
                        help="Increasingly verbose output (e.g. -v -vv -vvv),"
                        "for debugging use -vvv")
    args = parser.parse_args()
    
    verbose = args.verbose
    
    kmall_filename = args.kmall_filename
    kmall_directory = args.kmall_directory
    verify = args.verify
    
    if kmall_directory:       
        filestoprocess = []     
        suffix = "kmall"
        if verbose >= 3:
            print("directory: " + directory)
            
        # Recursively work through the directory looking for kmall files.
        for root,subFolders, files in os.walk(kmall_directory):
            for fileval in files:
                if fileval[-suffix.__len__():] == suffix:
                    filestoprocess.append(os.path.join(root,fileval))
    else:
        filestoprocess = [kmall_filename]

    if filestoprocess.__len__() == 0:
        print("No files found to process.")
        sys.exit()
    
    
    for filename in filestoprocess:
        print("")
        print("Processing: %s" % filename)
        
        # Create the class instance.
        K = kmall(filename )
        K.verbose = args.verbose
        if (K.verbose >= 1):	
            print("Processing file: %s" %  K.filename)
    
        pingcheckdata = []
        navcheckdata = []
        # Index file (check for index)
        K.index_file()
        if verify:
            K.report_packet_types()
            pingcheckdata.append([x for x in K.check_ping_count()])
            
            K.extract_attitude()    
            # Report gaps in attitude data.
            dt_att = np.diff([x.timestamp() for x in K.att["datetime"]])
            navcheckdata.append([np.min(np.abs(dt_att)),
                                  np.max(dt_att),
                                  np.mean(dt_att),
                                  1.0/np.mean(dt_att),  
                                  sum(dt_att >= 1.0)])    
            #print("Navigation Gaps min: %0.3f, max: %0.3f, mean: %0.3f (%0.3fHz)" % 
            #      (np.min(np.abs(dt_att)),np.max(dt_att),np.mean(dt_att),1.0/np.mean(dt_att)))
            #print("Navigation Gaps >= 1s: %d" % sum(dt_att >= 1.0))
    print("Packet statistics:")
    
    # Print column headers
    #print('%s' % "\t".join(['File','Npings','NpingsMissing','NMissingMRZ'] + 
    #                         ['Nav Min Time Gap','Nav Max Time Gap', 'Nav Mean Time Gap','Nav Mean Freq','Nav N Gaps >1s']))
    
    # Print columns
    #for x,y in zip(pingcheckdata,navcheckdata):
    #    row = x+y
    #    #print(row)
    #    print("\t".join([str(x) for x in row]))
       
    # Create DataFrame to make printing easier. 
    DataCheck = pd.DataFrame([x+y for x,y in zip(pingcheckdata,navcheckdata)],columns=
                             ['File','Npings','NpingsMissing','NMissingMRZ'] + 
                             ['NavMinTimeGap','NavMaxTimeGap', 'NavMeanTimeGap','NavMeanFreq','NavNGaps>1s'])
            #K.navDataCheck = pd.DataFrame(navcheckdata,columns=['Min Time Gap','Max Time Gap', 'Mean Time Gap','Mean Freq','N Gaps >1s'])
    pd.set_option('display.max_columns' , 30)
    pd.set_option('display.expand_frame_repr', False)
    print(DataCheck)
    #print(DataCheck)

