import json

class AddressInfo:
    def __init__(self, accessComments=None, addressLine1=None,addressLine2=None,contactEmail=None,contactTelephone1=None,contactTelephone2=None,
                 countryID=None,distanceUnit=None,ID=None,lat=None,long=None,postcode=None,relatedUrl=None, state=None,title=None,town=None,
                 distanceFromCurrentWaypoint=None, trip_distance=None):
            self.AccessComments = accessComments
            self.AddressLine1 = addressLine1
            self.AddressLine2 = addressLine2
            self.ContactEmail = contactEmail
            self.ContactTelephone1 = contactTelephone1
            self.ContactTelephone2 = contactTelephone2
            self.CountryID = countryID
            self.DistanceUnit = distanceUnit
            self.ID = ID
            self.Latitude = lat
            self.Longitude = long
            self.Postcode = postcode
            self.RelatedURL = relatedUrl
            self.StateOrProvince = state
            self.Title = title
            self.Town = town
            self.DistanceFromCurrentWaypoint = distanceFromCurrentWaypoint
            self.TripDistance = trip_distance
    
def to_json(instance):
    return {k:v for k , v in vars(instance).items() if not str(k).startswith('_')}

class Connection:
    def __init__(self, amps=None, connectionTypeId = None, currentTypeId = None, ID = None, levelID = None, powerKw = None, quantity=None, statusTypeID=None, voltage =None):
        self.Amps = amps
        self.ConnectionTypeID = connectionTypeId
        self.CurrentTypeID = currentTypeId
        self.ID = ID
        self.LevelID = levelID
        self.PowerKw = powerKw
        self.Quantity = quantity
        self.StatusTypeID = statusTypeID
        self.Voltage = voltage

class MediaItem:
    def __init__(self, user=None, chargePointID = None, comment = None, dateCreate =None, ID = None, isEnabled = None, isExternalResource = None, isFeaturedItem = None,
                 isVideo=None, itemThumbnailURL = None, itemURL = None):
        self.ChargePointID = chargePointID
        self.Comment = comment
        self.DateCreated = dateCreate
        self.ID = ID
        self.IsEnabled = isEnabled
        self.IsExternalResource = isExternalResource
        self.IsFeaturedItem = isFeaturedItem
        self.IsVideo = isVideo
        self.ItemThumbnailURL = itemThumbnailURL
        self.ItemURL = itemURL
        self.User = user

class User:
    def __init__(self, ID, profileImageURL=None, reputationPoints=None, username=None):
         self.ID=ID
         self.ProfileImageURL=profileImageURL
         self.ReputationPoints = reputationPoints
         self.Username = username

class UserComment:
    def __init__(self,user=None, chargePointsID=None, checkInStatusTypeID=None,commentTypeID=None, dateCreated=None,ID=None,rating=None,userName=None):
        self.ChargePointID = chargePointsID
        self.CheckinStatusTypeID = checkInStatusTypeID
        self.CommentTypeID = commentTypeID
        self.DateCreated = dateCreated
        self.ID = ID
        self.Rating = rating
        self.User=user
        self.UserName=userName
            
class Charger:
    def __init__(self, addressInfo, connections, mediaItems = None, userComments = None, dataProviderID=None, dataQualityLevel = None, dateCreated=None,dateLastStatusUpdate=None,
                 dateLastVerified=None,generalComments=None,ID=None,isRecentlyVerified=None,numberOfPoints=None,operatorID=None,statusTypeID=None,submissionStatusTypeID=None,
                 uuid = None, usageCost = None, usageTypeID= None):
        self.AddressInfo = addressInfo
        self.Connections = connections
        self.DataProviderID = dataProviderID
        self.DataQualityLevel = dataQualityLevel
        self.DateCreated = dateCreated
        self.DateLastStatusUpdate = dateLastStatusUpdate
        self.DateLastVerified = dateLastVerified
        self.GeneralComments = generalComments
        self.ID = ID
        self.IsRecentlyVerified = isRecentlyVerified
        self.MediaItems = mediaItems
        self.NumberOfPoints = numberOfPoints
        self.OperatorID = operatorID
        self.StatusTypeID = statusTypeID
        self.SubmissionStatusTypeID = submissionStatusTypeID
        self.UUID = uuid
        self.UsageCost = usageCost
        self.UsageTypeID = usageTypeID
        self.UserComments = userComments

def address_decoder(addressInfo):
    return AddressInfo(addressInfo['AccessComments'] if 'AccessComments' in addressInfo else None,
        addressInfo['AddressLine1'] if 'AddressLine1' in addressInfo else None,
        addressInfo['AddressLine2'] if 'AddressLine2' in addressInfo else None,
        addressInfo['ContactEmail'] if 'ContactEmail' in addressInfo else None,
        addressInfo['ContactTelephone1'] if 'ContactTelephone1' in addressInfo else None,
        addressInfo['ContactTelephone2'] if 'ContactTelephone2' in addressInfo else None,
        addressInfo['CountryID'] if 'CountryID' in addressInfo else None,
        addressInfo['DistanceUnit'] if 'DistanceUnit' in addressInfo else None,
        addressInfo['ID'] if 'ID' in addressInfo else None,
        addressInfo['Latitude'] if 'Latitude' in addressInfo else None,
        addressInfo['Longitude'] if 'Longitude' in addressInfo else None,
        addressInfo['Postcode'] if 'Postcode' in addressInfo else None,
        addressInfo['RelatedUrl'] if 'RelatedUrl' in addressInfo else None,
        addressInfo['StateOrProvince'] if 'StateOrProvince' in addressInfo else None,
        addressInfo['Title'] if 'Title' in addressInfo else None,
        addressInfo['Town'] if 'Town' in addressInfo else None)

def object_decoder(obj):
    if 'AddressInfo' in obj:
        addressInfo = obj['AddressInfo']
        a_info= address_decoder(addressInfo)

    if "Connections" in obj:
        connections = []
        for c in obj['Connections']:
            connections.append(Connection(
                c["Amps"] if 'Amps' in c else None,
                c["ConnectionTypeID"] if 'ConnectionTypeID' in c else None,
                c["CurrentTypeID"] if 'CurrentTypeID' in c else None,
                c["ID"] if 'ID' in c else None,
                c["LevelID"] if 'LevelID' in c else None,
                c["PowerKW"] if 'PowerKW' in c else None,
                c["Quantity"] if 'Quantity' in c else None,
                c["StatusTypeID"] if 'StatusTypeID' in c else None,
                c["Voltage"] if 'Voltage' in c else None,
            ))

    mediaItems = []
    if "MediaItems" in obj:
        for m in obj["MediaItems"]:
            try:
                mediaItems.append(MediaItem(
                    User(m["User"]["ID"],
                         m["User"]["ProfileImageURL"] if 'ProfileImageURL' in m['User'] else None,
                         m["User"]["ReputationPoints"] if 'ReputationPoints' in m['User'] else None,
                         m["User"]["Username"] if 'Username' in m['User'] else None
                    ) if 'User' in m else None,
                    m["ChargePointID"] if 'ChargePointID' in m else None,
                    m["Comment"] if 'Comments' in m else None,
                    m["DateCreated"] if 'DateCreated' in m else None,
                    m["ID"] if 'ID' in m else None,
                    m["IsEnabled"] if 'IsEnabled' in m else None,
                    m["IsExternalResource"] if 'IsExternalResource' in m else None,
                    m["IsFeaturedItem"] if 'IsFeaturedItem' in m else None,
                    m["IsVideo"] if 'IsVideo' in m else None,
                    m["ItemThumbnailURL"] if 'ItemThumbnailURL' in m else None,
                    m["ItemURL"] if 'ItemURL' in m else None
                ))
            except Exception as e:
                print('Media Item Exception: ', e)
                print(m)
    userComments = []         
    if "UserComments" in obj:
        for u in obj["UserComments"]:
            try:
                userComments.append(UserComment(
                    User(u["User"]["ID"],
                         u["User"]["ProfileImageURL"] if 'ProfileImageURL' in u['User'] else None,
                         u["User"]["ReputationPoints"] if 'ReputationPoints' in u['User'] else None,
                         u["User"]["Username"] if 'Username' in u['User'] else None
                    ) if 'User' in u else None,
                    u["ChargePointID"] if 'ChargePointID' in u else None,
                    u["CheckinStatusTypeID"] if 'CheckinStatusTypeID' in u else None,
                    u["CommentTypeID"] if 'CommentTypeID' in u else None,
                    u["DateCreated"] if 'DateCreated' in u else None,
                    u["ID"] if 'ID' in u else None,
                    u["Rating"] if 'Rating' in u else None,
                    u["UserName"] if 'UserName' in u else None
                ))
            except Exception as e:
                print('User Comment Exception: ', e)
                print(u)

    charger = Charger(a_info, connections, mediaItems, userComments,
        obj["DataProviderID"] if 'DataProviderID' in obj else None,
        obj["DataQualityLevel"] if 'DataQualityLevel' in obj else None,
        obj["DateCreated"] if 'DateCreated' in obj else None,
        obj["DateLastStatusUpdate"] if 'DateLastStatusUpdate' in obj else None,
        obj["DateLastVerified"] if 'DateLastVerified' in obj else None,
        obj["GeneralComments"] if 'GeneralComments' in obj else None,
        obj["ID"] if 'ID' in obj else None,
        obj["IsRecentlyVerified"] if 'IsRecentlyVerified' in obj else None,
        obj["NumberOfPoints"] if 'NumberOfPoints' in obj else None,
        obj["OperatorID"] if 'OperatorID' in obj else None,
        obj["StatusTypeID"] if 'StatusTypeID' in obj else None,
        obj["SubmissionStatusTypeID"] if 'SubmissionStatusTypeID' in obj else None,
        obj["UUID"] if 'UUID' in obj else None,
        obj["UsageCost"] if 'UsageCost' in obj else None,
        obj["UsageTypeID"] if 'UsageTypeID' in obj else None
        )

    return charger


def get_us_charge_locations(filepath='data/us_charge_data_simple.json'):
    print('Getting json data')
    with open(filepath, 'r') as f:
        data = json.loads( f.read())
        chargers = []
        for c in data:
            chargers.append(object_decoder(c))
            
    print(len(chargers))
    return chargers