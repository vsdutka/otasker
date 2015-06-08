// owaclassic
package otasker

func newOwaClassicProcRunner() func(f func(op *operation), streamID string) OracleTasker {
	const (
		stmEvalSessionID = `
declare
  l_sid varchar2(40);
begin
  :server_bg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  begin
    l_sid:=kill_session.get_current_session_id;
  exception
    when no_data_found then
      l_sid := null;
    when too_many_rows then
      l_sid := null;
  end;
  commit;
  :sid := l_sid;
  :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
end;
`
		stmMain = `
Declare
  rc__ number;
  l_num_params number;
  l_param_name owa.vc_arr;
  l_param_val owa.vc_arr;
  l_num_ext_params number;
  l_ext_param_name owa.vc_arr;
  l_ext_param_val owa.vc_arr;
  l_package_name varchar(240);
  %s
Begin
  :server_bg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  /* >> Инициализация параметров */
%s
  /* << Инициализация параметров */
  %s
  owa.init_cgi_env(l_num_params, l_param_name, l_param_val);
%s
  %s(%s);
  %s
  if (wpg_docload.is_file_download) then
    rc__ := 1;
    :content__ := '';
    :bNextChunkExists := 0;
    declare
      l_doc_info varchar2(32000);
      l_lob blob := :lob__;
      l_bfile bfile;
    begin
      wpg_docload.get_download_file(l_doc_info);
      if l_doc_info='ЕКБ' then
        null;
      elsif l_doc_info='B' then
        hrslt.GET_INFO(:ContentType ,:ContentLength,:CustomHeaders);
        wpg_docload.get_download_blob(l_lob);
        :lob__ := l_lob;
      elsif l_doc_info='F' then
        hrslt.GET_INFO(:ContentType ,:ContentLength,:CustomHeaders);
        wpg_docload.get_download_bfile(l_bfile);
        DBMS_LOB.LOADFROMFILE(l_lob, l_bfile, DBMS_LOB.getLength(l_bfile));
        :lob__ := l_lob;
      else
        declare
          l_len number;
          l_rest varchar2(32000) := l_doc_info;
          l_fn varchar2(32000);
          l_ct varchar2(4000);
          p_doctable varchar2(32000);
          sql_stmt varchar2(32000);
          cursor_handle INTEGER;
          retval INTEGER;
        begin
          l_len :=to_number('0'||substr(l_doc_info,1, instr(l_doc_info,'X', 1)-1));
          l_fn := substr(l_doc_info,instr(l_doc_info,'X', 1)+1, l_len);
          p_doctable := owa_util.get_cgi_env('DOCUMENT_TABLE');
          IF (p_doctable IS NULL) THEN
             p_doctable := 'wwv_document';
          END IF;

          sql_stmt := 'select nvl(MIME_TYPE,CONTENT_TYPE), blob_content  from '||p_doctable||
            ' where NAME=:docname';
          cursor_handle := dbms_sql.open_cursor;
          dbms_sql.parse(cursor_handle, sql_stmt, dbms_sql.v7);

          dbms_sql.define_column(cursor_handle, 1, l_ct, 128);
          dbms_sql.define_column(cursor_handle, 2, l_lob);
          dbms_sql.bind_variable(cursor_handle, ':docname', l_fn);

          retval := dbms_sql.execute_and_fetch(cursor_handle,TRUE);

          dbms_sql.column_value(cursor_handle, 1, l_ct);
          dbms_sql.column_value(cursor_handle, 2, l_lob);
          dbms_sql.close_cursor(cursor_handle);
          :ContentType := l_ct;
          :ContentLength := dbms_lob.getlength(l_lob);
          :CustomHeaders := '';
          :lob__ := l_lob;

        end;
      end if;
    end;
    commit;
    dbms_session.modify_package_state(dbms_session.reinitialize);
  else
    rc__ := 0;
    commit;
    hrslt.GET_INFO(:ContentType ,:ContentLength,:CustomHeaders);
    :content__ := hrslt.GET32000(:bNextChunkExists);
    if :bNextChunkExists = 0 then
      dbms_session.modify_package_state(dbms_session.reinitialize);
    end if;
  end if;
  commit;
  :rc__ := rc__;
  :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
  :sqlerrcode := 0;
  :sqlerrm := '';
  :sqlerrtrace := '';
exception
  when others then
    rollback;
    :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
    :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
	:sqlerrcode := SQLCODE;
	:sqlerrm := sqlerrm;
	:sqlerrtrace := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE();
end;`

		stmGetRestChunk = `begin
  :server_bg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :Data:=hrslt.GET32000(:bNextChunkExists);
  if :bNextChunkExists = 0 then
    dbms_session.modify_package_state(dbms_session.reinitialize);
  end if;
  commit;
  :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
  :sqlerrcode := 0;
  :sqlerrm := '';
  :sqlerrtrace := '';
exception
  when others then
    rollback;
    :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
    :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
	:sqlerrcode := SQLCODE;
	:sqlerrm := sqlerrm;
	:sqlerrtrace := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE();
end;`
		stmKillSession = `
begin
  kill_session.session_id:=:sess_id;
  :ret:=kill_session.kill_session_by_session_id(:out_err_msg);
end;
`
		stmFileUpload = `
declare
  l_item_id varchar2(40) := :item_id;/*Для совместимости*/
  l_application_id varchar2(40) := :application_id;/*Для совместимости*/
  l_page_id varchar2(40) := :page_id;/*Для совместимости*/
  l_session_id varchar2(40) := :session_id;/*Для совместимости*/
  l_request varchar2(40) := :request;/*Для совместимости*/
begin
  :server_bg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  owa.init_cgi_env(:num_params, :param_name, :param_val);
  %s
  insert into %s(name, mime_type, doc_size, last_updated, content_type, blob_content, pt_dc_id)
  values(:name, :mime_type, :doc_size, sysdate, :content_type, :lob, pt_dc_by_user());
  :ret_name := :name;
  :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
  :sqlerrcode := 0;
  :sqlerrm := '';
  :sqlerrtrace := '';
exception
  when others then
    rollback;
    :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
    :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ 0;
	:sqlerrcode := -20000;
	:sqlerrm := 'Unable to upload file "'||:name||'" '||sqlerrm;
	:sqlerrtrace := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE();
end;`
	)

	return func(f func(op *operation), streamID string) OracleTasker {
		return newOracleProcTasker(f, stmEvalSessionID, stmMain, stmGetRestChunk, stmKillSession, stmFileUpload, streamID)
	}
}
