import psycopg2 as db
import psycopg2.sql as sql
from psycopg2.extras import RealDictCursor

class ChargerContext:
    def __init__(self, host="localhost", dbname="evvehicle", user="evvehicle", password="ev-vehicle"):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect(self):
        connection = db.connect(host=self.host, dbname=self.dbname, user=self.user, password=self.password)
        connection.autocommit = True
        return connection

    def createDatabase(self):
        conn = self.connect()
        # create a cursor object called cur
        cur = conn.cursor()
                
        # construct a query string
        strSql = """
        --DO
        --$do$
        --BEGIN
            CREATE EXTENSION IF NOT EXISTS postgis; -- enable extension 
            
            CREATE TABLE IF NOT EXISTS ev.AddressInfo
            (
                ID INTEGER NOT NULL PRIMARY KEY,
                AccessComments VARCHAR(5000),
                AddressLine1 VARCHAR(200),
                AddressLine2 VARCHAR(200),
                ContactEmail VARCHAR(500),
                ContactTelephone1 VARCHAR(50),
                ContactTelephone2 VARCHAR(50),
                CountryID INTEGER,
                DistanceUnit INTEGER,
                Latitude FLOAT,
                Longitude FLOAT,
                Postcode VARCHAR(50),
                RelatedURL VARCHAR(500),
                StateOrProvince VARCHAR(100),
                Title VARCHAR(500),
                Town VARCHAR(500),
                Location GEOGRAPHY(Point, 4326)
            );

            CREATE INDEX geo_spx ON ev.AddressInfo USING GIST (Location);
        --END
        --$do$

        --DO 
        --$do$
        --BEGIN
            CREATE TABLE IF NOT EXISTS ev.Charger(
                ID INTEGER NOT NULL PRIMARY KEY,
                AddressInfoID INTEGER NOT NULL REFERENCES ev.AddressInfo(ID),
                DataProviderID INTEGER,
                DataQualityLevel INTEGER,
                DateCreated VARCHAR(50),
                DateLastStatusUpdate VARCHAR(50),
                DateLastVerified VARCHAR(50),
                GeneralComments VARCHAR(1000),
                IsRecentlyVerified BOOLEAN,
                NumberOfPoints INTEGER,
                OperatorID INTEGER,
                StatusTypeID INTEGER,
                SubmissionStatusTypeID INTEGER,
                UUID VARCHAR(200),
                UsageCost VARCHAR(500),
                UsageTypeID INTEGER
            );
        --END
        --$do$

        --DO 
        --$do$
        --BEGIN
            CREATE TABLE IF NOT EXISTS ev.Connection
            (
                ID INTEGER NOT NULL PRIMARY KEY,
                ChargerID INTEGER NOT NULL REFERENCES ev.Charger(ID),
                Amps INTEGER,
                ConnectionTypeID INTEGER,
                CurrentTypeID INTEGER,
                LevelID INTEGER,
                PowerKw FLOAT,
                Quantity INTEGER,
                StatusTypeID INTEGER,
                Voltage INTEGER
            );
        --END
        --$do$

        --DO 
        --$do$
        --BEGIN
            CREATE TABLE IF NOT EXISTS ev.OcmUser
            (
                ID INT NOT NULL PRIMARY KEY,
                ProfileImageURL VARCHAR(500),
                ReputationPoints INTEGER,
                Username VARCHAR(200)
            );
        --END
        --$do$

        --DO 
        --$do$
        --BEGIN
            CREATE TABLE IF NOT EXISTS ev.MediaItem
            (
                ID INTEGER NOT NULL PRIMARY KEY,
                ChargePointID INTEGER NOT NULL REFERENCES ev.Charger(ID),
                Comment VARCHAR(500),
                DateCreated VARCHAR(50),
                IsEnabled BOOLEAN,
                IsExternalResource BOOLEAN,
                IsFeaturedItem BOOLEAN,
                IsVideo BOOLEAN,
                ItemThumbnailURL VARCHAR(500),
                ItemURL VARCHAR(500),
                UserID INTEGER REFERENCES ev.OcmUser(ID)
            );
        --END
        --$do$

        --DO 
        --$do$
        --BEGIN
            CREATE TABLE IF NOT EXISTS ev.UserComment
            (
                ID INTEGER NOT NULL PRIMARY KEY,
                ChargePointID INTEGER,
                CheckinStatusTypeID INTEGER,
                CommentTypeID INTEGER,
                DateCreated VARCHAR(50),
                Rating INTEGER,
                UserID INTEGER REFERENCES ev.OcmUser(ID),
                Username VARCHAR(200)
            );
        --END
        --$do$
        """
        # execute the query
        try:
            cur.execute(strSql)
        except Exception as e: 
            print(e)
                
        cur.close()
        conn.commit()
        conn.close()

    def insertChargers(self, chargers):
        conn = self.connect()

        for charger in chargers:
            query = """
                INSERT INTO ev.AddressInfo 
                (
                    AccessComments,
                    AddressLine1,
                    AddressLine2,
                    ContactEmail,
                    ContactTelephone1,
                    ContactTelephone2,
                    CountryID,
                    DistanceUnit,
                    ID,
                    Latitude,
                    Longitude,
                    Postcode,
                    RelatedURL,
                    StateOrProvince,
                    Title,
                    Town
                ) 
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ev.AddressInfo WHERE ID = %s);
            """
            try:
                # create a cursor object called cur
                cur = conn.cursor()
                cur.execute(query, (charger.AddressInfo.AccessComments, charger.AddressInfo.AddressLine1,charger.AddressInfo.AddressLine2,
                            charger.AddressInfo.ContactEmail,charger.AddressInfo.ContactTelephone1,charger.AddressInfo.ContactTelephone2,
                            charger.AddressInfo.CountryID,charger.AddressInfo.DistanceUnit,charger.AddressInfo.ID,charger.AddressInfo.Latitude,
                            charger.AddressInfo.Longitude,charger.AddressInfo.Postcode,charger.AddressInfo.RelatedURL,charger.AddressInfo.StateOrProvince,
                            charger.AddressInfo.Title,charger.AddressInfo.Town, charger.AddressInfo.ID))
            except Exception as e:
                print('Address Exception: ', e)
            cur.close()
            conn.commit()

            query = """
                UPDATE ev.AddressInfo SET Location=st_SetSrid(st_MakePoint(Longitude, Latitude), 4326) WHERE ID = {0};
            """.format(charger.AddressInfo.ID)
            try:
                cur = conn.cursor()
                cur.execute(sql.SQL(query))
            except Exception as e:
                print('Update address info error: ', e)
            cur.close()
            conn.commit()

            chargerInsertQuery = """
                INSERT INTO ev.Charger
                (
                    ID,
                    AddressInfoID,
                    DataProviderID,
                    DataQualityLevel,
                    DateCreated,
                    DateLastStatusUpdate,
                    DateLastVerified,
                    GeneralComments,
                    IsRecentlyVerified,
                    NumberOfPoints,
                    OperatorID,
                    StatusTypeID,
                    SubmissionStatusTypeID,
                    UUID,
                    UsageCost,
                    UsageTypeID
                )
                SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                WHERE NOT EXISTS (SELECT 1 FROM ev.Charger WHERE ID = %s);
            """
            try:
                cur = conn.cursor()
                cur.execute(chargerInsertQuery, (charger.ID, charger.AddressInfo.ID, charger.DataProviderID,
                                                charger.DataQualityLevel, charger.DateCreated, charger.DateLastStatusUpdate,
                                                charger.DateLastVerified, charger.GeneralComments, charger.IsRecentlyVerified,
                                                charger.NumberOfPoints, charger.OperatorID, charger.StatusTypeID, charger.SubmissionStatusTypeID, 
                                                charger.UUID, charger.UsageCost,charger.UsageTypeID, charger.ID))
            except Exception as e:
                print("Charger exception: ", e)
                print(charger.DateCreated, ' ', charger.DateLastStatusUpdate, ' ', charger.DateLastVerified, ' ', charger.UsageCost)

            cur.close()
            conn.commit()
            
            for c in charger.Connections:
                connectionInsertQuery ="""
                INSERT INTO ev.Connection 
                (
                    ID,
                    ChargerID,
                    Amps,
                    ConnectionTypeID,
                    CurrentTypeID,
                    LevelID,
                    PowerKw,
                    Quantity,
                    StatusTypeID,
                    Voltage
                ) 
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ev.Connection WHERE ID = %s);
                """
                try:
                    cur = conn.cursor()
                    cur.execute(connectionInsertQuery, (c.ID, charger.ID, c.Amps, c.ConnectionTypeID, c.CurrentTypeID, c.LevelID, c.PowerKw, c.Quantity,
                                c.StatusTypeID, c.Voltage, c.ID))
                except Exception as e:
                    print('Connection insert exception: ', e)

                cur.close()
                conn.commit()

            for mi in charger.MediaItems:
                if mi.User != None:
                    mediaUserInsertQuery = """
                        DO
                        $do$
                        BEGIN
                            CREATE EXTENSION IF NOT EXISTS dblink; -- enable extension 
                            IF NOT EXISTS(SELECT (NULL) FROM ev.OcmUser WHERE ID = {0})
                            THEN
                                INSERT INTO ev.OcmUser
                                (
                                    ID,
                                    ProfileImageURL,
                                    ReputationPoints,
                                    Username
                                )
                                VALUES(%s, %s, %s, %s);
                            END IF;
                        END
                        $do$
                            """.format(mi.User.ID)
                    try:
                        cur = conn.cursor()
                        cur.execute(mediaUserInsertQuery, (mi.User.ID, mi.User.ProfileImageURL, mi.User.ReputationPoints, mi.User.Username))
                    except Exception as e:
                        print('media item user insert exception: ', e)

                    cur.close()
                    conn.commit()

                mediaInsertQuery = """
                        INSERT INTO ev.MediaItem
                        (
                            ID,
                            ChargePointID,
                            Comment,
                            DateCreated,
                            IsEnabled,
                            IsExternalResource,
                            IsFeaturedItem,
                            IsVideo,
                            ItemThumbnailURL,
                            ItemURL,
                            UserID
                        )
                        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s 
                        WHERE NOT EXISTS (SELECT 1 FROM ev.MediaItem WHERE Id = %s);

                    """

                try:
                    cur = conn.cursor()
                    params = (mi.ID,mi.ChargePointID, mi.Comment, mi.DateCreated,
                                        mi.IsEnabled, mi.IsExternalResource, mi.IsFeaturedItem,
                                        mi.IsVideo, mi.ItemThumbnailURL, mi.ItemURL,
                                        mi.User.ID, mi.ID)
                    cur.execute(mediaInsertQuery , params)
                except Exception as e:
                    print('Media item insert exception: ', e)
                    print('User: ', mi.User.ID, mi.User.ProfileImageURL, mi.User.ReputationPoints, mi.User.Username)
                    print('MI: ', mi.ID,mi.ChargePointID, mi.Comment, mi.DateCreated,
                                        mi.IsEnabled, mi.IsExternalResource, mi.IsFeaturedItem,
                                        mi.IsVideo, mi.ItemThumbnailURL, mi.ItemURL,
                                        mi.User.ID)

                cur.close()
                conn.commit()

            for uc in charger.UserComments:
                if uc.User != None:
                    try:
                        userCommentUserInsertQuery = """
                        DO
                        $do$
                        BEGIN
                            IF NOT EXISTS(SELECT (NULL) FROM ev.OcmUser WHERE ID = {0})
                            THEN
                                INSERT INTO ev.OcmUser
                                (
                                    ID,
                                    ProfileImageURL,
                                    ReputationPoints,
                                    Username
                                )
                                Values(%s, %s, %s, %s);
                            END IF;
                        END
                        $do$
                            """.format(uc.User.ID)

                        cur = conn.cursor()
                        cur.execute(userCommentUserInsertQuery, (uc.User.ID, uc.User.ProfileImageURL, uc.User.ReputationPoints, uc.User.Username))
                    except Exception as e:
                        print('usercomment user insert exception: ',e)
                        print(uc.User)
                        print(uc)
                    cur.close()
                    conn.commit()
                    
                userCommentInsertQuery = """
                        INSERT INTO ev.UserComment
                        (
                            ID,
                            ChargePointID,
                            CheckinStatusTypeID,
                            CommentTypeID,
                            DateCreated,
                            Rating,
                            UserID,
                            Username
                        )
                        SELECT %s,%s,%s,%s, %s,%s,%s,%s
                        WHERE NOT EXISTS (SELECT 1 FROM ev.UserComment WHERE ID = %s);
                    """

                try:
                    cur = conn.cursor()
                    params = (
                        uc.ID, 
                        uc.ChargePointID, 
                        uc.CheckinStatusTypeID,
                        uc.CommentTypeID, 
                        uc.DateCreated, 
                        uc.Rating,
                        uc.User.ID if uc.User != None else None,
                        uc.UserName,
                        uc.ID)
                    
                    cur.execute(userCommentInsertQuery, params)
                except Exception as e:
                    print('Usercomment insert exception: ',e)

                cur.close()
                conn.commit()
        conn.close()

    def dropDatabase(self):
        conn = self.connect()
        # create a cursor object called cur
        cur = conn.cursor()
                
        # construct a query string
        strSql = """
        DROP TABLE ev.usercomment;
        DROP TABLE ev.mediaitem;
        DROP TABLE ev.OcmUser;
        DROP TABLE ev.connection;
        DROP TABLE ev.charger;
        DROP TABLE ev.addressinfo;
        """
        cur.execute(strSql)
        cur.close()
        conn.commit()
        conn.close()

    def getNearestChargers(self, point = [41.7370,-111.8338], distance=3):
        raise "TODO: Fix query to use points."
        nearest_point_query = """
            WITH line AS ( SELECT ST_GeomFromText('LINESTRING(-111.8338 41.73711,
                -111.83484 41.73713,
                -111.83489 41.7352,
                -111.83495 41.73415,
                -111.83497 41.73341,
                -111.83497 41.73328,
                -111.83497 41.73317,
                -111.83498 41.73273,
                -111.83498 41.73233,
                -111.83499 41.73193,
                -111.835 41.73149,
                -111.83501 41.73138,
                -111.83501 41.73125,
                -111.83502 41.73114,
                -111.83504 41.73047,
                -111.83504 41.73018,
                -111.83505 41.73009,
                -111.83505 41.72996,
                -111.83505 41.72993,
                -111.83506 41.72956,
                -111.83507 41.7293,
                -111.83507 41.72902,
                -111.83508 41.72869,
                -111.8351 41.72775,
                -111.83513 41.72659,
                -111.83514 41.72602,
                -111.83514 41.72593,
                -111.8352 41.72402,
                -111.83526 41.72215,
                -111.83528 41.72116,
                -111.8353 41.72024,
                -111.83534 41.71927,
                -111.83539 41.71808,
                -111.83554 41.71762,
                -111.8356 41.71733,
                -111.83564 41.71706,
                -111.83575 41.71671,
                -111.83588 41.71637,
                -111.83606 41.71604,
                -111.83618 41.71584,
                -111.83648 41.71536,
                -111.83678 41.71487,
                -111.83681 41.71471,
                -111.83712 41.71433,
                -111.83734 41.71409,
                -111.83769 41.71375,
                -111.83815 41.71334,
                -111.83859 41.71301,
                -111.83944 41.71237,
                -111.83965 41.71221,
                -111.84064 41.71145,
                -111.84221 41.71027,
                -111.84272 41.70989,
                -111.84294 41.70973,
                -111.84442 41.70865,
                -111.84605 41.70742,
                -111.84822 41.70588,
                -111.84894 41.70537,
                -111.84913 41.70524,
                -111.84951 41.70494,
                -111.85063 41.70413,
                -111.85159 41.70342,
                -111.8528 41.70255,
                -111.85492 41.70101,
                -111.8561 41.70012,
                -111.85692 41.69952,
                -111.85748 41.69911,
                -111.85811 41.69865,
                -111.85873 41.69823,
                -111.85899 41.69804,
                -111.85974 41.69753,
                -111.86059 41.69692,
                -111.86115 41.69646,
                -111.86205 41.6958,
                -111.86222 41.69568,
                -111.86303 41.69504,
                -111.86349 41.69473,
                -111.8645 41.69389,
                -111.86543 41.69309,
                -111.8657 41.69283,
                -111.86732 41.69117,
                -111.87181 41.68669,
                -111.87301 41.6855,
                -111.87422 41.6843,
                -111.87587 41.6826,
                -111.87746 41.68103,
                -111.87878 41.6797,
                -111.87916 41.67933,
                -111.87959 41.6789,
                -111.88029 41.67821,
                -111.88133 41.67715,
                -111.88306 41.67539,
                -111.88766 41.6708,
                -111.89104 41.66737,
                -111.89449 41.66399,
                -111.89695 41.66165,
                -111.90005 41.65875,
                -111.90423 41.65448,
                -111.90805 41.65083,
                -111.90975 41.64924)', 4326) AS geom)
                
            SELECT
                c.ID AS ID,
                c.DataProviderID AS DataProviderID,
                c.DataQualityLevel AS DataQualityLevel,
                c.DateCreated AS DateCreated,
                c.DateLastStatusUpdate AS DateLastStatusUpdate,
                c.DateLastVerified AS DateLastVerified,
                c.GeneralComments AS GeneralComments,
                c.IsRecentlyVerified AS IsRecentlyVerified,
                c.NumberOfPoints AS NumberOfPoints,
                c.OperatorID AS OperatorID,
                c.StatusTypeID AS StatusTypeID,
                c.SubmissionStatusTypeID AS SubmissionStatusTypeID,
                c.UUID AS UUID,
                c.UsageCost AS UsageCost,
                c.UsageTypeID AS UsageTypeID,
                ai.ID AS AddressInfoID, 
                ai.AccessComments AS AccessComments,
                ai.AddressLine1 AS AddressLine1,
                ai.AddressLine2 AS AddressLine2,
                ai.ContactEmail AS ContactEmail,
                ai.ContactTelephone1 AS ContactTelephone1,
                ai.ContactTelephone2 AS ContactTelephone2,
                ai.CountryID AS CountryID,
                ai.DistanceUnit AS DistanceUnit,
                ai.Latitude AS Latitude,
                ai.Longitude AS Longitude,
                ai.Postcode AS Postcode,
                ai.RelatedURL AS RelatedURL,
                ai.StateOrProvince AS StateOrProvince,
                ai.Title AS Title,
                ai.Town AS Town,
                ST_Y(ST_ClosestPoint((SELECT geom FROM line), ST_MakePoint(ai.longitude, ai.latitude)::geography::geometry)) AS intersectionlatitude,
                ST_X(ST_ClosestPoint((SELECT geom FROM line), ST_MakePoint(ai.longitude, ai.latitude)::geography::geometry)) AS intersectionlongitude,
                ST_Distance(ST_MakePoint(ai.longitude, ai.latitude)::geography, (SELECT geom FROM line)) AS DistanceFromRoute
            FROM
                ev.charger c
                JOIN ev.addressinfo ai on c.addressinfoid = ai.id
            WHERE 
                ST_DWithin(ST_MakePoint(ai.longitude, ai.latitude)::geography, (SELECT geom FROM line), 8046.72);
        """.format(point[1], point[0], distance * 1609.344)

        conn = self.connect()
        # create a cursor object called cur
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(nearest_point_query)
        charger_data = cur.fetchall()

        conn.commit()
        cur.close()
        conn.close()

        return charger_data
        