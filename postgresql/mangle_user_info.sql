BEGIN;

CREATE OR REPLACE FUNCTION equity.mangle_email_addr() RETURNS TRIGGER AS '
BEGIN
	IF NEW.email != ''etin@company.com''
		AND NEW.email != ''ebrin@company.com''
		AND NEW.email != ''rgone@company.com''
	THEN
		NEW.email := REPLACE(NEW.email, ''@'', ''_at_'')||''@company.com'';
	END IF;
	RETURN NEW;
END;
' LANGUAGE plpgsql;

CREATE TRIGGER qa_mangle_email_addr
	BEFORE INSERT OR UPDATE
	ON equity.pentaho_user_account_map
	FOR EACH ROW
	EXECUTE PROCEDURE equity.mangle_email_addr();

CREATE OR REPLACE FUNCTION equity.mangle_password() RETURNS TRIGGER AS '
BEGIN
	IF NEW.username != ''etin''
		AND NEW.username != ''ebrin''
		AND NEW.username != ''rgone''
		AND NEW.username != ''hstev''
	THEN
		NEW.password := CRYPT(CRYPT(NEW.password, GEN_SALT(''md5'')), GEN_SALT(''md5''));
	END IF;
	RETURN NEW;
END;
' LANGUAGE plpgsql;

CREATE TRIGGER qa_mangle_password
	BEFORE INSERT OR UPDATE
	ON equity.users
	FOR EACH ROW
	EXECUTE PROCEDURE equity.mangle_password();

COMMIT;
