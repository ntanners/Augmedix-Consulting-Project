# Augmedix Consulting Project

## Augmedix - Unified data to detect IoT outages

### Company Description:
Augmedix is on a mission to rehumanize healthcare by improving the patient-doctor interaction and thereby the overall quality of care delivered. 

#### How does it work? 
Doctors wear Google Glass and stream audio and video to Augmedix throughout the day. Behind the scenes, remote human experts create the medical notes for patient visits in real time, uploading the final notes to the EHR.

Through the Augmedix platform we are able to save doctors an average of two to three hours per day. 

### Problem Description:
The Augmedix service essentially uses Google Glass as an IOT device, relying on low latency high throughput streaming of audio/video and other data over clinic WiFi environments. We generate tremendous amount of data related to how doctors interact with our devices and the general status of those devices in the field. This data is largely unstructured. When there is an issue causing a doctor to be offline, the root cause analysis requires digging through high volumes of data with ambiguous clarity. 

Our goal is to structure the various streams of data into a consumable and repeatable process of root causes analysis, and ultimately enabling a system to predict when there could be an issue in connectivity.

### Data Description:
Augmedix is able to track multiple elements (but not all) of wifi connectivity, scribe connectivity, server health, bluetooth connectivity, stream state, actions taken by clinicians on their devices, actions taken by documentation specialists on the web applications etc. This data is generally addressed as “log data” from different tools, apps, systems and users. Streaming and sensitive data is persisted for a minimum of three days.

### Prior Work:
Initial tools to help create a single source of truth have been created (primarily SQL queries of various DBs, but not a unified view of the connectivity). This can be leveraged to quickly understand the various elements relevant to each type of data.

### Deliverable:
A unified source of data for investigations that can be utilized to rapidly resolve issues (predict the root cause) and if possible, predict probability of an issue.

### Implementation:
None. We’re open and excited to understand the tradeoffs of various technologies and which approach would be chosen to solve this problem. Our desire is to find a solution that can be integrated into our platform and maintained / developed further to help solve this problem over time, which might limit some too far out there ideas, but are interested to learn what is the cutting edge.
