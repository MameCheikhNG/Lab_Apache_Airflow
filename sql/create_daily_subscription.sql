USE activation_DB

CREATE TABLE daily_subscription (
    customer VARCHAR(255),
    lastname VARCHAR(255),
    firstname VARCHAR(255),
    sim BIGINT,
    msisdn BIGINT,
    libelleprofil VARCHAR(255),
    date_souscription VARCHAR(255),
    typeclient VARCHAR(255),
    identity_number VARCHAR(255),
    identity_desc VARCHAR(255),
    Etat_identification VARCHAR(255),
    Etat_ligne VARCHAR(255),
    FileName VARCHAR(255)
);