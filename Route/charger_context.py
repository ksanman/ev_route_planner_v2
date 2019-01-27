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
        nearest_point_query = """
        SELECT 
            c.ID as ID,
            c.AddressInfoID as AddressInfoID,
            c.DataProviderID as DataProviderID,
            c.DataQualityLevel as DataQualityLevel,
            c.DateCreated as DateCreated,
            c.DateLastStatusUpdate as DateLastStatusUpdate,
            c.DateLastVerified as DateLastVerified,
            c.GeneralComments as GeneralComments,
            c.IsRecentlyVerified as IsRecentlyVerified,
            c.NumberOfPoints as NumberOfPoints,
            c.OperatorID as OperatorID,
            c.StatusTypeID as StatusTypeID,
            c.SubmissionStatusTypeID as SubmissionStatusTypeID,
            c.UUID as UUID,
            c.UsageCost as UsageCost,
            c.UsageTypeID as UsageTypeID,
            ai.ID as ID, 
            ai.AccessComments as AccessComments,
            ai.AddressLine1 as AddressLine1,
            ai.AddressLine2 as AddressLine2,
            ai.ContactEmail as ContactEmail,
            ai.ContactTelephone1 as ContactTelephone1,
            ai.ContactTelephone2 as ContactTelephone2,
            ai.CountryID as CountryID,
            ai.DistanceUnit as DistanceUnit,
            ai.Latitude as Latitude,
            ai.Longitude as Longitude,
            ai.Postcode as Postcode,
            ai.RelatedURL as RelatedURL,
            ai.StateOrProvince as StateOrProvince,
            ai.Title as Title,
            ai.Town as Town,
            ST_Distance(ai.Location, ST_MakePoint({0},{1})::geography) as DistanceFromCurrentWaypoint
        FROM 
            ev.AddressInfo ai
            JOIN ev.Charger c ON ai.ID = C.AddressInfoID
            JOIN ev.Connection co ON c.ID = co.ChargerID
                AND co.LevelID = 3
        WHERE
            ST_DWithin(ai.Location, ST_MakePoint({0},{1})::geography, {2})
        ORDER BY
            ST_Distance(ai.Location, ST_MakePoint({0},{1})::geography)
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
        