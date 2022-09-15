from lxml import etree
from Utility import *
import numpy as np
import sys
import datetime
from YearValidate import validate_year
import multiprocessing
import copyreg
from functools import partial

def init(args0):
    global SQ14_min_tow_rate_list
    SQ14_min_tow_rate_list = args0

def element_unpickler(data):
    return etree.fromstring(data)

def element_pickler(element):
    data = etree.tostring(element)
    return element_unpickler, (data,)

copyreg.pickle(etree._Element, element_pickler, element_unpickler)

def elementtree_unpickler(data):
    return etree.parse(BytesIO(data))

def elementtree_pickler(tree):
    return elementtree_unpickler, (etree.tostring(tree),)

copyreg.pickle(etree._ElementTree, elementtree_pickler, elementtree_unpickler)

def inputreader(inputfile, LOFS42_exc_use, RATE_DISTRIBUTION, DOW_FLAG, pooling_inputreader):
    try:
        rootElement = etree.parse(inputfile)
    except:
        raise Exception('Usage: XMLReader:Read - XML is missing root element')
        
    GUA_REST_DNI_FLAG='Y'
    if 'bkt' in rootElement.getroot().attrib:
        bkt = rootElement.getroot().attrib['bkt']
        if len(bkt) > 0:
            if ((bkt == 'Y') or (bkt == 'y')):
                GUA_REST_DNI_FLAG = 'Y'
            else:
                GUA_REST_DNI_FLAG = 'N'
     
    LOFS14_max_obs_rates = MAX_OBSERVED_RATES  # this should be defined in GUIN1 to allow for the switch to be set            

    try:
        costList = rootElement.findall('cost')
    except:
        raise Exception('Usage: XMLReader:Read - There are no cost elements')

    cost_mars = []

    if pooling_inputreader:
        SQ14_min_tow_rate_list = multiprocessing.Manager().list()         
    else:
        SQ14_min_tow_rate_list = []
        init(SQ14_min_tow_rate_list)
    SQ14_min_tow_rate_list.append(MAX_OBSERVED_RATES)

    if len(costList) > 0:
        if pooling_inputreader:
            pool = multiprocessing.Pool(processes=8, initializer = init, initargs=(SQ14_min_tow_rate_list,))
            cost_mars = pool.map(partial(multi_read_cost_mars, LOFS42_exc_use = LOFS42_exc_use, DOW_FLAG = DOW_FLAG, RATE_DISTRIBUTION=RATE_DISTRIBUTION), costList)
            pool.close()
            pool.join()  
        else:
            cost_mars = [multi_read_cost_mars(costElement, LOFS42_exc_use, DOW_FLAG, RATE_DISTRIBUTION) for costElement in costList]

    return cost_mars, max(SQ14_min_tow_rate_list), GUA_REST_DNI_FLAG    


def multi_read_cost_mars(costElement, LOFS42_exc_use, DOW_FLAG, RATE_DISTRIBUTION):
            global SQ14_min_tow_rate_list
            cost = Cost_mar()
            cost_attributes = costElement.attrib

            cost.ignore = 'N'
            cost.n_exceptions = 0
        
            if 'id' in cost_attributes:
                cid = costElement.attrib['id']
                if (len(cid) > 0):
                    cid = cid[:CUST_ID_LEN]
                else:
                    raise Exception('XMLReader:Read - Cost_mar id attribute value is empty')
        
            else :
                raise Exception('XMLReader:Read - Cost_mar id attribute is missing')
            
            file_input = open(f'input_print/{cid}_out.out', 'w')
            sys.stdout = file_input
            cost.id = cid
            cost.bkt = 'Y'
            cost.bucket_length = -1
        
            if ('tol' in cost_attributes):
                tol = costElement.attrib['tol']
            
                if (len(tol) > 0):
                    try:
                        cost.indicator_full_point = float(tol)
                    except:
                        cost.ignore='Y'
                        ReportError(cost, "Cost_mar tol attribute value is invalid -- cost_mar (" + cid + ") ignored")
                        #continue
                        return cost
                else:
                    cost.ignore='Y'
                    ReportError(cost, "Cost_mar tol attribute value is empty -- cost_mar (" + str(cid) + ") ignored ")
                    #continue
                    return cost
            else:
                cost.indicator_full_point = sys.float_info.max
        
        
            if ('cap' in cost_attributes):
                cap = costElement.attrib['cap']
                if (len(cap) > 0):
                    try:
                        cost.capacity = float(cap)
                    except:
                        cost.ignore='Y'
                        ReportError(cost, "Cost_mar cap attribute value is invalid -- cost_mar (" + str(cid) + ") ignored ")   
                        #continue
                        return cost
                else:
                    cost.ignore='Y'
                    ReportError(cost, "Cost_mar cap attribute value is empty -- cost_mar (" + str(cid) + ") ignored ")
                    #continue
                    return cost
            else:
                cost.ignore='Y'
                ReportError(cost, "Cost_mar cap attribute is missing -- cost_mar (" + str(cid) + ") ignored ")
                #continue
                return cost 
            
            if ('stdf' in cost_attributes):
                stdf = costElement.attrib['stdf']
                if (len(stdf) > 0):
                    try:
                        cost.old_standard_fill = max(0,float(stdf))
                        if ((cost.old_standard_fill > cost.capacity) and cost.capacity > 0):
                            cost.old_standard_fill = cost.capacity * 0.9
                    except:
                        cost.ignore='Y'
                        ReportError(cost, "Cost_mar stdf attribute value is invalid -- cost_mar (" + str(cid) + ") ignored")
                        #continue
                        return cost
                else:
                    cost.ignore='Y'
                    ReportError(cost, "Cost_mar stdf attribute value is empty -- cost_mar (" + str(cid) + ") ignored ")
                    #continue
                    return cost
            else:
                cost.ignore='Y'
                ReportError(cost, "Cost_mar stdf attribute is missing -- cost_mar (" + str(cid) + ") ignored ")
                #continue
                return cost 
        
            if ('ur' in cost_attributes):
                ur = costElement.attrib['ur']
                cost.old_use_rate_null = 'N'
                if (len(ur) > 0):
                    try:
                        cost.old_use_rate = float(ur)
                    except:
                        cost.ignore='Y'
                        ReportError(cost, "Cost_mar ur attribute value is invalid -- cost_mar (" + str(cid) + ") ignored")
                        #continue
                        return cost
                else:
                    # PALS sends null when cost_mar is initialized, so can't ignore if value is null, 7/20/12, awd
                    ReportError(cost, "Cost_mar ur attribute value is empty -- cost_mar (" + str(cid) + ")")
                    # DF needs to know that a null was originally sent, 6/10/15, cme
                    cost.old_use_rate_null = 'Y'
                    cost.old_use_rate = 0
            else:
                cost.ignore='Y'
                ReportError(cost, "Cost_mar ur attribute is missing -- cost_mar (" + str(cid) + ") ignored ")
                #continue
                return cost 
        
            cost.fixed_method = -1   # default - program will pick the best method
        
            if ('mthd' in cost_attributes):
                mthd = costElement.attrib['mthd']
                if (len(mthd) > 0):
                    try:
                        mthd = int(mthd)
                        if (mthd == -1 or (mthd > 0 and mthd < 10)):
                            cost.fixed_method = mthd
                        else:
                            cost.ignore='Y'
                            ReportError(cost, "Cost_mar mthd attribute value is invalid -- cost_mar (" + str(cid) + ") ignored")
                            #continue
                            return cost                        
                    except:
                        pass
        
            cost.binLength = -1 # no usage bin will be calculated
            cost.mmf = 'Y' # multimode by default
        
            profilesList = costElement.findall('profiles') 
        
            if len(profilesList) == 0 :
                ReportError(cost, "Profiles node missing for the cost_mar - cost_mar (" + str(cid) + ") ignored")
                cost.ignore='Y'
                #continue
                return cost
            
            profileList = profilesList[0].findall('profile')
            cost.n_profiles = len(profileList)
        
            if cost.n_profiles == 0:
                ReportError(cost, "No profiles specified for the cost_mar - cost_mar (" + str(cid) + ") ignored")
                cost.ignore='Y'
                #continue
                return cost
            
            cost.rms = [] # len(profileList)*[Profile()]
        
            for r in range(cost.n_profiles):
                profileElement = profileList[r]
                rm = Profile()
                if ('rid' in profileElement.attrib):
                    rid = profileElement.attrib['rid']
                    if (len(rid) > 0):
                        rm.rid = rid[:REGIME_ID_LEN]
                    else:
                        ReportError(cost, "Profile rid attribute is empty - cost_mar (" + str(cid) + ") ignored")
                        cost.ignore = 'Y'
                        continue
                else:
                    ReportError(cost, "Profile rid attribute is missing - cost_mar (" + str(cid) + ") ignored")
                    cost.ignore = 'Y'
                    continue     
            
                modesList = profileElement.find('modes')
                if (modesList is None):
                    ReportError(cost, "no modes definition for profile")
                    cost.ignore = 'Y'
                    continue
                modeList = modesList.findall('mode')
                if (len(modeList)!=0):
                    rm.n_modes = len(modeList)
                else:
                    ReportError(cost, "The cost_mar does not have any modes specified")
                    cost.ignore = 'Y'
                    continue
                        
                rm.oms = []
                for modeElement in modeList:
                    om = OprMode()
                    om.is_in_pattern = 0
                    if ('mid' in modeElement.attrib):
                        mid = modeElement.attrib['mid']
                        if (len(mid) > 0):
                            om.omid = mid[:OM_ID_LEN]
                        else:
                            ReportError(cost, "Mode mid attribute value is empty - cost_mar (" + str(cid) + ") ignored")
                            cost.ignore = 'Y'
                            continue
                    else:
                        ReportError(cost, "Mode mid attribute is missing - cost_mar (" + str(cid) + ") ignored")
                        cost.ignore = 'Y'
                        continue
                    
                    om.rid = rid[:REGIME_ID_LEN]

                    om.fixed_use_rate = -1
                    if ('fxur' in modeElement.attrib):
                        fixedur = modeElement.attrib['fxur']
                        if (len(fixedur) > 0):
                            try:
                                om.fixed_use_rate = max(0, float(fixedur))  # must be >= zero
                            except:
                                pass
                            
                    om.old_forecast_status = 0
                    if ('fs' in modeElement.attrib):
                        fs = modeElement.attrib['fs']
                        if ((len(fs) > 0) and (0 <= int(fs) <= 4)):
                            om.old_forecast_status = int(fs)
                                   
                    if ('lut' in modeElement.attrib):
                        lut = modeElement.attrib['lut']
                        lut = lut[:DATE_TIME_LEN]
                        lut = validate_year(lut)
                        result, dt, t = cc36_to_julian_date_time(lut)
                        if (result == True):
                            om.date_last_update = dt
                            om.time_last_update = t
                        else:
                            ReportError(cost, "Invalid last update date/time for mode")
                        
                    om.bucket_length = -1
                    om.bucket = 'Y'
                
                    rm.oms.append(om)
                    # end of modes
                
                # read in the generations 
                gensList = profileElement.findall('gens')          
                if (gensList is None or len(gensList) == 0):
                    ReportError(cost, "No generation definition for profile")
                    cost.ignore = 'Y'
                    continue

                genList = gensList[0].findall('gen')
                if (len(genList)==0):
                    ReportError(cost, "Invalid generation definition for profile")
                    cost.ignore = 'Y'
                    continue
                else:
                    rm.n_gens = len(gensList)
                    
                
                rm.gens = [] #len(genList) * [Generation()]
                for genElement in genList:
                    gen = Generation()
                    if 'gid' in genElement.attrib:
                        gid = genElement.attrib['gid']
                        if (len(gid) > 0):
                            gen.gid = gid[:GEN_ID_LEN]    
                        else:
                            gen.gid = ""

                    gen.rid = rm.rid
                        
                    if 'effT' in genElement.attrib:
                        effT = genElement.attrib['effT']
                        if (len(effT) == 0):
                            cost.ignore = 'Y'
                            ReportError(cost, "Effective time of generation is missing for cost_mar (" + str(cid) + ")")
                            rm.n_gens = rm.n_gens - 1
                            continue
                        date_time = effT[:DATE_TIME_LEN]
                        date_time = validate_year(date_time)
                        julian_date_time = cc36_to_julian_date_time(date_time)
                        if (julian_date_time[0] == False):
                            ReportError(cost, "Invalid date/time for generation in cost_mar (" + str(cid) + ")")
                            cost.ignore = 'Y'
                            rm.n_gens = rm.n_gens - 1
                            continue
                    
                        date, time = julian_date_time[1], julian_date_time[2]
                        gen.date_start = date
                        gen.time_start = time
                        gen.date_end = INT_MAX
                        gen.time_end = INT_MAX
                
                    else:
                        cost.ignore = 'Y'
                        ReportError(cost, "Effective time of generation is missing for cost_mar (" + str(cid) + ")")
                        rm.n_gens = rm.n_gens - 1
                        continue                    
                
                    for g in range(len(genList)):
                        if (g > 0):
                            rm.gens[g-1].date_end = date
                            rm.gens[g-1].time_end = time
                            if (g == cost.rms[r].n_gens -1):
                                rm.gens[g].date_end = INT_MAX
                                rm.gens[g].time_end = INT_MAX
                    
                        ompList = genElement.getchildren()
                       
                        if (len(ompList) ==0):
                            raise ValueError('XMLReader:Read - Failed to get the omp list -- program exits')          
                    
                        omp = OprModePattern() 
                        omp.omp = np.zeros((7,24),dtype=object)
                    
                        for op in range(len(ompList)):
                            if etree.iselement(ompList[op]):
                                ompElement = ompList[op]
                        
                            if 'dow' in ompElement.attrib:
                                dow = ompElement.attrib['dow']
                                if (len(dow) > 0):
                                    cDow = dow[:255]    #+'\0'
                                    n_days, days = cc38_spread_day_time(cDow)
                                
                            if 'hrs' in ompElement.attrib:
                                hrs = ompElement.attrib['hrs']
                                if (len(hrs) > 0):
                                    cHrs = hrs[:255]    #+'\0'
                                    n_hours, hours = cc38_spread_day_time(cHrs)
                                
                        
                            if 'mid' in ompElement.attrib:
                                mid = ompElement.attrib['mid']
                                cMid = mid[:OM_ID_LEN]
                            
                                for d in range(n_days):
                                    for h in range(n_hours):
                                        if (mid != ""):
                                            omp.omp[days[d]-1, hours[h]-1] = cMid
                
                        gen.omps = omp
                   
                    # end of generations

                    # find the maximum operating hours for each operating mode
                    # also capture the day of week for the max operating hours, 9/11/14, cme
                
                    if (cost.ignore == "N"):
                        for m in range(rm.n_modes):
                            max_mode_daily_op_hrs = 0
                            max_op_hrs_dow = 0
                            om = rm.oms[m]
                            for g in range(rm.n_gens):                 
                                omp = gen.omps
                                for i in range(7):
                                    daily_op_hrs = 0
                                    for j in range(24):
                                        if (om.omid == omp.omp[i][j]):
                                            daily_op_hrs += 1
                                            om.is_in_pattern = 1
                                    
                                    if (daily_op_hrs > max_mode_daily_op_hrs):
                                        max_op_hrs_dow = i
                                    max_mode_daily_op_hrs = max(max_mode_daily_op_hrs,daily_op_hrs)    
                    
                    
                            om.max_op_hrs = max_mode_daily_op_hrs
                            om.max_op_hrs_dow = max_op_hrs_dow

                    rm.gens.append(gen)
               
                rassnsList = profileElement.findall('rassns')
            
                if(rassnsList is None or len(rassnsList)==0):
                    ReportError(cost, "No profile assignments specified for profile")
                    cost.ignore='Y'
                    continue
                
                rassnList = rassnsList[0].getchildren()
                if (len(rassnList) > 0):
                    rm.n_rassns = len(rassnList)
                else:
                    rm.n_rassns = 0
                    
                if(rm.n_rassns == 0):
                    ReportError(cost, "At least 1 profile assignment must be specified for each profile")
                    cost.ignore='Y'
                    continue
                    
                rm.rassns = [] #rm.n_rassns * [ProfileAssignment()]

                
                for m in range(len(rassnList)):
                    if etree.iselement(rassnList[m]):
                        rassnElement = rassnList[m]
                        
                    rassn = ProfileAssignment() #rm.rassns[m]
                    rassn.rid = rm.rid
                    
                    if 'st' in rassnElement.attrib:
                        st = rassnElement.attrib['st']
                        if (len(st)==0):
                            cost.ignore = 'Y'
                            ReportError(cost, "Profile assignment st attribute is empty for cost_mar (" + str(cid) + ") - profile assignment ignored")
                            rm.n_rassns = rm.n_rassns - 1
                            continue
                        
                        date_time = st[:DATE_TIME_LEN]
                        date_time = validate_year(date_time)

                        julian_date_time = cc36_to_julian_date_time(date_time)
                        if (julian_date_time[0] == False):
                            cost.ignore = 'Y'
                            ReportError(cost, "Profile assignment st attribute is invalid for cost_mar (" + str(cid) + ") - profile assignment ignored")
                            rm.n_rassns = rm.n_rassns - 1
                            continue
                            
                        rassn.date_start = julian_date_time[1]
                        rassn.time_start = julian_date_time[2]
                    
                    else:
                        cost.ignore = 'Y'
                        ReportError(cost, "Profile assignment st attribute is missing for cost_mar (" + str(cid) + ") - profile assignment ignored")
                        rm.n_rassns = rm.n_rassns - 1
                        continue
                        
                    if 'et' in rassnElement.attrib:
                        et = rassnElement.attrib['et']
                        if (len(et)==0):
                            cost.ignore = 'Y'
                            ReportError(cost, "Profile assignment et attribute is empty for cost_mar (" + str(cid) + ") - profile assignment ignored")
                            rm.n_rassns = rm.n_rassns - 1
                            continue
                            
                        date_time = et[:DATE_TIME_LEN]
                        date_time = validate_year(date_time)

                        julian_date_time = cc36_to_julian_date_time(date_time)
                        if (julian_date_time[0] == False):
                            cost.ignore = 'Y'
                            ReportError(cost, "Profile assignment et attribute is invalid for cost_mar (" + str(cid) + ") - profile assignment ignored")
                            rm.n_rassns = rm.n_rassns - 1
                            continue
                        
                        rassn.date_end = julian_date_time[1]
                        rassn.time_end = julian_date_time[2]
                        
                        
                    else:
                        cost.ignore = 'Y'
                        ReportError(cost, "Profile assignment et attribute is missing for cost_mar (" + str(cid) + ") - profile assignment ignored")
                        rm.n_rassns = rm.n_rassns - 1
                        continue

                    rm.rassns.append(rassn)
                    

                
                cost.rms.append(rm)
            
            ############################################################## profile end ##########################################

            eupsList = costElement.findall("eups")
            irsList = costElement.findall("irs")
        
            if (len(irsList) == 0):
                ReportError(cost, "irs node missing for the cost_mar - cost_mar ignored")
                cost.ignore='Y'
                #continue
                return cost
            
            irList = irsList[0].getchildren()
        
            # if the exc_use switch is sent, it means include all time of usage, regardless of exc use periods and 
            # regardless of profiles; this provides actual consumption, cme, 1/18/2016
            # also skip all exceptional use when PALS calls DF from the RunDOW method, 8/15/2016
        
            if((not LOFS42_exc_use) and (DOW_FLAG != 'Y')): 
                # create dummy exceptional use periods between profile assignments, cme, 2/19/2013
                if ((eupsList is not None) or (cost.mmf == 'Y')):
                    if (len(eupsList)!=0):
                        eupList = eupsList[0].getchildren()
                    cost.n_exceptions = len(eupList)
                    if (cost.mmf == 'Y' and len(rassnsList) > 0):  
                        if(rm.n_rassns > 0):
                            # need to find the number of profile assignments per profile
                            maxRassns, totExc = 0, 0
                            for reg in range(cost.n_profiles):
                                tempCount = 0
                                rm = cost.rms[reg]
                                tempCount = rm.n_rassns
                                totExc = totExc + tempCount-1
                                if (tempCount > maxRassns):
                                    maxRassns = tempCount
                            if (maxRassns > 1):
                                cost.n_exceptions = cost.n_exceptions + totExc
                        
                
                    #   Read the exception use period records for this cost_mar
                    if (cost.n_exceptions > 0):
                        cost.exceptions = []
                        # using both m and n was needed in the C program to free memory when records failed the validation
                        # m had tracked the actual number of eup records in the input file, while n tracked the eups that passed validation
                        # since this python version does not need to free memory, the n is removed
                        for m in range(cost.n_exceptions):
                            excp = Cust_exception() #cost.exceptions[n]
                        
                            # first read all the REAL eup's, 2/19/2013
                            if (m < len(eupList)):
                                eupElement = eupList[m]
                                if (len(eupList) > 0):
                                    if('ur' in eupElement.attrib):                                
                                        ur = eupElement.attrib['ur']
                                        if (ur is not None and len(ur) > 0):
                                            try:
                                                excp.use_rate = float(ur)
                                            except:
                                                ReportError(cost, "ur invalid for eup, exceptional use period ignored")
                                                cost.n_exceptions -= 1
                                                continue
                                        else:                                            
                                            ReportError(cost, "ur not specified for eup, exceptional use period ignored")
                                            cost.n_exceptions -= 1
                                            continue
                                    
                                        if ('st' in eupElement.attrib):
                                            st = eupElement.attrib['st']
                                            if (st is not None and len(st) > 0):
                                                date_time = st[:DATE_TIME_LEN] 
                                                date_time = validate_year(date_time)                                           
                                                dtstart = datetime.datetime.strptime(date_time, '%Y-%m-%d-%H:%M')
                                            else:                                                
                                                ReportError(cost, "Start time not specified for eup, exceptional use period ignored")
                                                cost.n_exceptions -= 1
                                                continue
                                    
                                            julian_date_time = cc36_to_julian_date_time(date_time)
                                            if (julian_date_time[0] == False):
                                                ReportError(cost, "Invalid date/time for eup, exceptional use period ignored")
                                                cost.n_exceptions -= 1
                                                continue
                                        
                                            date, time = julian_date_time[1], julian_date_time[2]
                                            excp.date_start = date
                                            excp.time_start = time
                                        else:
                                            ReportError(cost, "Start time not specified for eup, exceptional use period ignored")
                                            cost.n_exceptions -= 1
                                            continue

                                        if ('et' in eupElement.attrib):
                                            et = eupElement.attrib['et']  
                                            if (et is not None and len(et) > 0):
                                                date_time = et[:DATE_TIME_LEN]
                                                date_time = validate_year(date_time)
                                                dtend = datetime.datetime.strptime(date_time, '%Y-%m-%d-%H:%M')
                                            else:
                                                ReportError(cost,"End time not specified for eup, exceptional use period ignored")
                                                cost.n_exceptions -= 1
                                                continue
                                        
                                            julian_date_time = cc36_to_julian_date_time(date_time)
                                            if (julian_date_time[0] == False):                                
                                                ReportError(cost, "Invalid date/time for eup, exceptional use period ignored")
                                                cost.n_exceptions -= 1
                                                continue

                                            date, time = julian_date_time[1], julian_date_time[2]
                                            excp.date_end = date
                                            excp.time_end = time
                                        else:
                                                ReportError(cost,"End time not specified for eup, exceptional use period ignored")
                                                cost.n_exceptions -= 1
                                                continue
                                
                                    else:
                                        ReportError(cost, "ur invalid for eup, exceptional use period ignored")
                                        cost.n_exceptions -= 1
                                        continue
                                 
                            else:
                                # then create fictitious eup's between profile assignments, 02/20/2013
                                if (cost.mmf == 'Y'):
                                    # need to find the number of profile assignments per profile
                                    for reg in range(cost.n_profiles):
                                        ed1, et1 = 0, 0
                                        rm = cost.rms[reg]
                                        for ra in range(rm.n_rassns-1,-1,-1):
                                            rassn = rm.rassns[ra]
                                            if (ed1 > 0):
                                                excp.use_rate = cost.old_use_rate
                                                if (excp.use_rate ==0):
                                                    excp.use_rate = 1
                                                excp.date_start = ed1
                                                excp.time_start = et1
                                                excp.date_end = rassn.date_start
                                                excp.time_end = rassn.time_start
                                            
                                            ed1 = rassn.date_end
                                            et1 = rassn.time_end

                                 # end creating dummy eup's

                            cost.exceptions.append(excp)
                    
            # end of exc_use switch 
            #                

            if (irList is not None):
                cost.n_indicators = len(irList)
            else:
                cost.n_indicators = 0
            
            #Loop through all the indicator readings to find out the no. of orders
            #This needs to be done because both order and indicator reading records
            #are merged together  and no of order is required to allocate an 
            #array of pointers to hold those orders 

            n_orders = 0
            for n in range(cost.n_indicators):
                irsElement = irList[n]
                if 'typ' in irsElement.attrib:
                    typ = irsElement.attrib['typ']
                    if (len(typ) > 0):
                        if (typ[0]=='E'):
                            if ('fillid' in irsElement.attrib):
                                fillid = irsElement.attrib['fillid']
                                if len(fillid.strip()) > 0:
                                    n_orders += 1
        
            cost.n_orders = 0
            cost.orders = []

            SQ14_min_tow_rate_list.append(cost.n_indicators)
            #LOFS14_max_obs_rates = max(LOFS14_max_obs_rates,cost.n_indicators)
            
            # Read ir records for this cost_mar into temporary collection and sort by inventory date time desc
            tempIndicators = []
            if (cost.n_indicators > 0):
                for n in range(cost.n_indicators):
                    indicator = Indicator_reading()
                    indicator.date = 32767
                    indicator.xmlExclude = False
                    irsElement = irList[n]
                    if 'n' in irsElement.attrib:
                        no = irsElement.attrib['n']
                        try:
                            indicator.inventory_no = int(no)
                        except:
                            indicator.inventory_no = -1
                            ReportError(cost, "Inventory number invalid, reading number set to -1")                            
                    else:
                        ReportError(cost, "Inventory number missing, reading number set to -1")
                        indicator.inventory_no = -1
                
                    # need to get type before inv to process deliveries appropriately, 9/23/2015, cme    
                    if 'typ' in irsElement.attrib:
                        typ = irsElement.attrib['typ']
                        if (typ is not None and len(typ) > 0):
                            indicator.type = typ[0]
                        else:
                            ReportError(cost, "Type missing for irs, reading " + str(indicator.inventory_no) + " ignored")
                            indicator.xmlExclude = True
                            cost.n_indicators -= 1
                            continue
                    else:
                        ReportError(cost, "Type missing for irs, reading " + str(indicator.inventory_no) + " ignored")
                        indicator.xmlExclude = True
                        cost.n_indicators -= 1
                        continue
                
                    entryInvalid = True
                    
                    indicator.ignore_reason_code = 0
                    indicator.break_seq = None
                    if 'inv' in irsElement.attrib:
                        inv = irsElement.attrib['inv']
                        if (inv is not None and len(inv) > 0):
                            try:
                                indicator.inventory = float(inv)
                                entryInvalid = False
                            except:
                                ReportError(cost, "Inventory invalid, reading " + str(indicator.inventory_no) + " ignored")
                        elif indicator.type != 'E':
                            ReportError(cost, "Inventory missing, reading " + str(indicator.inventory_no) + " ignored")
                        else:
                            # need to keep delivery end readings regardless of inventory so that DF knows a delivery occurred, 9/23/2015, cme
                            indicator.inventory = cost.capacity * 1.5
                            indicator.ignore_reason_code = MISSING_INV
                            entryInvalid = False
                            ReportError(cost, "Inventory missing on delivery end, reading " + str(indicator.inventory_no) + " set to exceed capacity")
                  
                    elif indicator.type != 'E':
                        ReportError(cost, "Inventory missing, reading " + str(indicator.inventory_no) + " ignored")
                    else:
                        # need to keep delivery end readings regardless of inventory so that DF knows a delivery occurred, 9/23/2015, cme
                        indicator.inventory = cost.capacity * 1.5
                        indicator.ignore_reason_code = MISSING_INV
                        entryInvalid = False
                        ReportError(cost, "Inventory missing on delivery end, reading " + str(indicator.inventory_no) + " set to exceed capacity")
                                    
                    if (entryInvalid):
                        indicator.xmlExclude = True
                        cost.n_indicators -= 1
                        continue
                
                    entryInvalid = True
                    if 'cap' in irsElement.attrib:
                        cap = irsElement.attrib['cap']
                        if (cap is not None and len(cap) > 0):
                            try:
                                indicator.capacity = float(cap)
                                entryInvalid = False
                            except:
                                ReportError(cost, "Capacity invalid, reading " + str(indicator.inventory_no) + " ignored")
                        else:
                            ReportError(cost, "Capacity missing, reading " + str(indicator.inventory_no) + " ignored")
                    else:
                        ReportError(cost, "Capacity missing, reading " + str(indicator.inventory_no) + " ignored")

                            
                    if (entryInvalid):
                        indicator.xmlExclude = True
                        cost.n_indicators -= 1
                        continue
                
                    if 't' in irsElement.attrib:
                        t = irsElement.attrib['t']
                        if (t is not None and len(t) > 0):
                            date_time = t[:DATE_TIME_LEN]
                            date_time = validate_year(date_time)
                        else:
                            ReportError(cost, "Invalid date/time for irs, reading " + str(indicator.inventory_no) + " ignored")
                            indicator.xmlExclude = True
                            cost.n_indicators -= 1
                            continue
                    
                        if ( cc36_to_julian_date_time(date_time)[0]==False):
                            ReportError(cost, "Invalid date/time for irs, reading " + str(indicator.inventory_no) + " ignored")
                            indicator.xmlExclude = True
                            cost.n_indicators -= 1
                            continue
            
                
                        julian_date_time = cc36_to_julian_date_time(date_time)
                        
                        indicator.date = julian_date_time[1]
                        indicator.time = julian_date_time[2]
                    else:
                        ReportError(cost, "Date/time missing for irs, reading " + str(indicator.inventory_no) + " ignored")
                        indicator.xmlExclude = True
                        cost.n_indicators -= 1
                        continue
                        
                    # For indicator reading type E create a new order
                    if (indicator.type == 'E'):
                        entryInvalid = False   # see note below, can't ignore any 'E' readings
                        if 'fillid' in irsElement.attrib:
                            fillid = irsElement.attrib["fillid"]
                        else:
                            ReportError(cost, "Fill Id missing for irs, reading " + str(indicator.inventory_no) + " set to indicator number")
                            fillid = str(n)
                        if(len(fillid.strip()) <= 0):
                            ReportError(cost, "Fill Id missing for irs, reading " + str(indicator.inventory_no) + " set to indicator number")                            
                            fillid = str(n)
                        if 'vol' in irsElement.attrib:
                            vol = irsElement.attrib["vol"]
                            if (vol is not None and len(vol) > 0):
                                if (len(vol.strip()) > 0):
                                    try:
                                        indicator.del_vol = float(vol)                                             
                                    # can't ignore any 'E' readings because then DF doesn't know a delivery occurred, 9/23/2015, cme
                                    #   DF needs to break the sequence whenever a delivery occurred
                                    except:
                                        ReportError(cost, "Volume invalid, reading " + str(indicator.inventory_no) + " set vol to std fill")
                                        indicator.del_vol = cost.old_standard_fill                                             
                                else:
                                    ReportError(cost, "Volume attribute is empty, reading " + str(indicator.inventory_no) + " set vol to std fill")
                                    indicator.del_vol = cost.old_standard_fill                                           
                            else:
                                ReportError(cost, "Volume missing, reading " + str(indicator.inventory_no) + " set vol to std fill")
                                indicator.del_vol = cost.old_standard_fill                                                                        
                                                       
                        else:
                            ReportError(cost, "Volume missing, reading " + str(indicator.inventory_no) + " set vol to std fill")
                            indicator.del_vol = cost.old_standard_fill                                                                     

                                
                    # For indicator reading of type E and S copy the fillid into so_number*/
                    if ((indicator.type == 'E') or (indicator.type == 'S')):  #qht
                        if 'fillid' in irsElement.attrib:
                            fillid = irsElement.attrib["fillid"]
                        else:                            
                            fillid = str(n)
                        if (fillid is not None and len(fillid) > 0):
                            indicator.so_number = fillid[:SO_LEN]
                        else:
                            indicator.so_number = str(n)
                    else:
                        indicator.so_number = ''    
                                
                    tempIndicators.append(indicator)
            
                # original code said "sort indicator readings desc"
                # However, the actual code in the original sorted it ascending!
                tempIndicators = sorted(tempIndicators, key=lambda e: (e.date, e.time))
        
                # Process the ir records for this cost_mar 
                cost.indicators = []
                m, n = -1, -1
                while n < cost.n_indicators-1:
                    n += 1
                    m += 1
                
                    if (tempIndicators[m].xmlExclude):
                        n -=1
                        cost.n_indicators -=1
                        continue
                
                    cost.indicators.append(tempIndicators[m])
                    indicator = tempIndicators[m] 
                
                    htime_data = tm()
                    
                    if(cost.ignore == "N"):
                        if (cost.mmf == 'Y' and len(rassnsList) > 0):
                            if(rm.n_rassns > 0):
                                day, mon, yr = cc14_cday(indicator.date)
                                htime_data.tm_sec, htime_data.tm_min, htime_data.tm_hour = 0, 0, 0
                                htime_data.tm_mday = day
                                htime_data.tm_mon = mon -1
                                htime_data.tm_year = yr
                                #print (day, mon, yr)
                                # htime_data.tm_wday from C uses Monday at day 1 whereas Python's weekday() uses Monday as day 0
                                # the data in the input file uses day 1 as Sunday, day 2 as Monday, etc. 
                                weekday = datetime.datetime(yr, mon, day).weekday()
                                if (weekday < 6):
                                    weekday += 1
                                else:
                                    weekday = 0
                    
                                start_hr = loct16_int_hr_min(indicator.time)

                                # if there were no profile assignments matching the indicator reading, use an arbitrary value, Nov. 8, 2021
                                indicator.gid = ""
                                indicator.omid = ""
                                indicator.rid = ""
                    
                                for r in range(cost.n_profiles):
                                    rm = cost.rms[r]
                                    # consider profile assignments, May 26, 2009
                                    for ra in range(rm.n_rassns):
                                        rassn = rm.rassns[ra]
                                        # this code assumed there would always exist a profile assignment that matches the indicator reading, Nov. 8, 2021
                                        if ((indicator.date >= rassn.date_start) and (indicator.date <= rassn.date_end)):
                                            for g in range(rm.n_gens):
                                                gen = rm.gens[g]
                                                omp = gen.omps
                                                if (indicator.date >= gen.date_start and indicator.date <= gen.date_end):
                                                    indicator.gid = gen.gid
                                                    # Hours are returned in a range of 0-23. So we can use the value directly.
                                                    if (start_hr == 24):
                                                        start_hr = 23
                                        
                                                    indicator.omid = omp.omp[weekday,int(start_hr)]
                                                    break
                                            indicator.rid = rassn.rid
                                            break                                                   

                                # By Default the cross mode rates would be ignored in multi mode.
                                if (rm.n_gens > 0):
                                    if ((n > 0) 
                                        and ((indicator.rid != cost.indicators[n-1].rid) or (indicator.gid !=cost.indicators[n-1].gid) or (indicator.omid != cost.indicators[n-1].omid))
                                        and (RATE_DISTRIBUTION == 1)):
                                        cost.indicators[n-1].break_seq = 'Y'
                
                    # For indicator reading type E create a new order
                    if (indicator.type == 'E'):
                        if ((indicator.so_number is not None) and (len(indicator.so_number) > 0)): 
                            ord = Order()
                            ord.del_vol = indicator.del_vol
                            ord.so_number = indicator.so_number[:SO_LEN]
                            ord.del_date = indicator.date
                            ord.del_time = indicator.time
                            ord.ignore = 'N'
                            cost.n_orders += 1
                            cost.orders.append(ord)
                    
                    if (indicator.ignore_reason_code == MISSING_INV):
                        indicator.ignore = 'Y'
                    else:
                        indicator.ignore = 'N'
                    
            # Set the pointers to order
            for n in range(cost.n_indicators):
                indicator = cost.indicators[n]
                indicator.order = None
                if ((indicator.type =='E') or (indicator.type == 'S')):
                    indicator = lofa18_add_order_ptr_indicator(cost,indicator)
                    #print ("indicatorpt", vars(indicator).keys())
                    cost.indicators[n] = indicator
        
            
            # set pointers to indicator readings from all costs orders */
            
            cost = lofa16_add_indicator_ptr_order_all(cost)
         
            # break sequence for the last reading in each profile assignment, cme, 02/20/2013
            # this is to ensure time sequencing is not performed between readings in different assignments

            if(cost.ignore == "N"):
                if(len(rassnsList) > 0):   
                    if(rm.n_rassns > 0):
                        for r in range(cost.n_profiles):
                            rm = cost.rms[r]
                            for ra in range(rm.n_rassns):
                                rassn = rm.rassns[ra]
                                tempDt, tempTm = 0, 0
                                tempGR = Indicator_reading()
                
                                for n in range(cost.n_indicators):
                                    indicator = cost.indicators[n]
                    
                                    if ((indicator.date <= rassn.date_end) 
                                    and (indicator.date >= rassn.date_start) 
                                    and ((indicator.date > tempDt) or (indicator.date == tempDt and indicator.time > tempTm))):
                                        tempDt = indicator.date
                                        tempTm = indicator.time
                                        tempGR = indicator
                        
                                if (tempGR is not None):
                                    tempGR.break_seq = 'Y'            
                    
            #Initializing the remaining fields of cost_mar structure to default
            cost.n_forecast_methods = 6
            cost.n_comb_methods = 3

            forecast_methods = []
            comb_methods = []

            for n in range(cost.n_forecast_methods):
                method = Forecast_method()
                if(n == 0):
                    method.name = "Cornell"[:METHOD_NAME_LEN]
                    method.id = CORNELL_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 0
                    method.std_dev_err = 0
                    method.wt_std_dev_err = 0
                    method.wt_avg_err = 0
                elif(n == 1):
                    method.name = "Wright"[:METHOD_NAME_LEN]
                    method.id = WRIGHT_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 1
                    method.std_dev_err = 1
                    method.wt_std_dev_err = 1
                    method.wt_avg_err = 1
                elif(n == 2):
                    method.name = "Holt"[:METHOD_NAME_LEN]
                    method.id = HOLT_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 2
                    method.std_dev_err = 2
                    method.wt_std_dev_err = 2
                    method.wt_avg_err = 2
                elif(n == 3):
                    method.name = "Moving Average"[:METHOD_NAME_LEN]
                    method.id = MOVE_AVG_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 3
                    method.std_dev_err = 3
                    method.wt_std_dev_err = 3
                    method.wt_avg_err = 3
                elif(n == 4):
                    method.name = "Sequential Discounted Regression"[:METHOD_NAME_LEN]
                    method.id = SEQ_DISC_REG_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 4
                    method.std_dev_err = 4
                    method.wt_std_dev_err = 4
                    method.wt_avg_err = 4
                elif(n == 5):
                    method.name = "Last Rate to Next Rate"[:METHOD_NAME_LEN]
                    method.id = LAST_TO_NEXT_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 5
                    method.std_dev_err = 5
                    method.wt_std_dev_err = 5
                    method.wt_avg_err = 5
                
                forecast_methods.append(method)

           
            for n in range(cost.n_comb_methods):
                method = Forecast_method()
                if(n == 0):
                    method.name = "MAE weights Combination"[:METHOD_NAME_LEN]
                    method.id = COMB_MAE_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 6
                    method.std_dev_err = 6
                    method.wt_std_dev_err = 6
                    method.wt_avg_err = 6
                elif(n == 1):
                    method.name = "MSE weights Combination"[:METHOD_NAME_LEN]
                    method.id = COMB_MSE_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 7
                    method.std_dev_err = 7
                    method.wt_std_dev_err = 7
                    method.wt_avg_err = 7
                elif(n == 2):
                    method.name = "Averaging Combination"[:METHOD_NAME_LEN]
                    method.id = COMB_AVG_ID
                    method.est_rates = []
                    method.dur_wt_mov_mse = 8
                    method.std_dev_err = 8
                    method.wt_std_dev_err = 8
                    method.wt_avg_err = 8
                    
                comb_methods.append(method)

            cost.forecast_methods = forecast_methods
            cost.comb_methods = comb_methods

            cost.errors = []
            file_input.close()
            return cost  
