// owaekb
package otasker

func NewOwaEkbProcRunner() func(f func(op *OracleOperation), streamID string) OracleTasker {
	const (
		stmEvalSessionID = `
declare
  l_sid varchar2(40);
begin
  :server_bg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  begin
    l_sid:=wskill_session.e_gcurrent_session_id;
  exception
    when no_data_found then
      l_sid := null;
    when too_many_rows then
      l_sid := null;
  end;
  :sid := l_sid;
  :server_fn := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  :server_fn_scn := /*sys.dbms_flashback.get_system_change_number*/ '0';
end;
`
		stmMain = `
Declare
  rc__ number;
  l_num_params number;
  l_param_name wscontext.et_vc_arr;
  l_param_val wscontext.et_vc_arr;
  l_num_ext_params number;
  l_ext_param_name wscontext.et_vc_arr;
  l_ext_param_val wscontext.et_vc_arr;
  l_package_name varchar(240);
  %s
Begin
  :server_bg := to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM');
  /* >> Инициализация параметров */
%s
  /* << Инициализация параметров */
  %s
  wscontext.e_init_cgi_env(l_num_params, l_param_name, l_param_val);
  wscontext.e_store_external_parameters(l_package_name, l_num_ext_params, l_ext_param_name, l_ext_param_val);
%s
  %s(%s);
  %s
  if (wsp.e_gIsFileDownload) then
    rc__ := 1;
    :content__ := '';

    declare
      l_lob blob := :lob__;
    begin
      :ContentType := '';
      :ContentLength := wsp.e_gContentLength;
      :CustomHeaders := wsp.e_gHTMLHdrs;
      wsp.e_Download_blob(l_lob);
      :lob__ := l_lob;
      :bNextChunkExists := 0;
    end;
    commit;
    dbms_session.modify_package_state(dbms_session.reinitialize);
  else
    rc__ := 0;
    commit;
    :ContentType := '';
    :ContentLength := wsp.e_gContentLength;
    :CustomHeaders := wsp.e_gHTMLHdrs;
    :content__ := wsp.e_gContentChunk(32000, :bNextChunkExists);
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
  :Data:=wsp.e_gContentChunk(32000, :bNextChunkExists);
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
  wskill_session.ev_Session_ID:=:sess_id;
  :ret:=wskill_session.e_kill_session_by_session_id(:out_err_msg);
exception
  when others then
    if sqlcode = -00031 then
	  :ret := 1;
	else
      :ret := 0;
      :out_err_msg := sqlerrm;
	end if;
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

	return func(f func(op *OracleOperation), streamID string) OracleTasker {
		return newOracleProcTasker(f, stmEvalSessionID, stmMain, stmGetRestChunk, stmKillSession, stmFileUpload, streamID)
	}
}
