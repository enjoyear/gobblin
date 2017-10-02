package gobblin.ingestion.zuora;

import java.io.Serializable;

import com.google.common.base.Strings;


public class ZuoraQuery implements Serializable {
  private static final long serialVersionUID = 1L;
  public String name;
  public String query;
  public String type = "zoqlexport";
  //Check the documentation here:
  //https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/BA_Stateless_and_Stateful_Modes
  public ZuoraDeletedColumn deleted = null;

  ZuoraQuery(String name, String query, String deleteColumn) {
    super();
    this.name = name;
    this.query = query;
    if (!Strings.isNullOrEmpty(deleteColumn)) {
      deleted = new ZuoraDeletedColumn(deleteColumn);
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
