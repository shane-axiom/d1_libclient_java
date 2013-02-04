package org.dataone.client.types;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.dataone.service.types.v1.Identifier;


/**
 * An object meant to contain information on the history of an object, including
 * all of the objects the given pid obsoletes, and objects that obsolete it. 
 * @author rnahf
 *
 */
public class ObsoletesChain {
    
	private Identifier startingPid;
	private List<Object[]> infoTable;
	private TreeMap<Long,Integer> byDateIndex;
	private Map<Identifier,Integer> byIdIndex;
	
	private final static int PID = 0;
	private final static int PUBLISH_DATE = 1;
	private final static int OBSOLETES = 2;
	private final static int OBSOLETEDBY = 3;
	private final static int IS_ARCHIVED = 4;
	
	public ObsoletesChain(Identifier pid) {
		this.startingPid = pid;
		this.infoTable = new LinkedList<Object[]>();
		this.byDateIndex = new TreeMap<Long, Integer>();
		this.byIdIndex = new HashMap<Identifier, Integer>();

	}
	
	public Identifier getStartingPoint() {
		return this.startingPid;
	}
	
	
	public void addObject(Identifier pid, Date publishDate, 
	Identifier obsoletes, Identifier obsoletedBy, Boolean isArchived) {
		if( publishDate == null) {
			throw new NullPointerException("publishDate parameter cannot be null.");
		}
		this.infoTable.add(new Object[]{pid,publishDate,obsoletes, obsoletedBy, isArchived});
		this.byDateIndex.put(publishDate.getTime(), this.infoTable.size()-1);
		this.byIdIndex.put(pid, this.infoTable.size()-1);
	}
	
//	public void removeObject(Identifier pid) {
		// THIS WILL BE DIFFICULT TO IMPLEMENT, AND PROBABLY NOT USEFUL
		// EASIER TO START OVER, EH?
//		Integer index = this.byIdIndex.get(pid);
//		this.byDateMap.remove(publishDate);
//		this.byIdMap.remove(pid);
//	}
	
	public Identifier getVersionAsOf(Date asOfDate) {
		Long asOf = asOfDate.getTime();
		Iterator<Long> it = this.byDateIndex.keySet().iterator();
		Long time = null;
		while (it.hasNext()) {
			Long nextTime = it.next();
			if (asOf < nextTime) {
				break;
			}
			time = nextTime;
		}
		if (time == null)
			return null;
		
		int tableIndex = this.byDateIndex.get(time);
		return (Identifier) this.infoTable.get(tableIndex)[PID];
	}
	
	public Identifier nextVersion(Identifier pid) {
		int tableIndex = this.byIdIndex.get(pid);
		return (Identifier) this.infoTable.get(tableIndex)[OBSOLETEDBY];
		
	}
	
	public Identifier previousVersion(Identifier pid) {
		int tableIndex = this.byIdIndex.get(pid);
		return (Identifier) this.infoTable.get(tableIndex)[OBSOLETES];
	}
	

	public Identifier getLatestVersion() {
		// this seems quite inefficient...
		return getByPosition(size()-1);
	}
		
	public Identifier getOriginalVersion() {
		return getByPosition(0);
		
	}
	
	public Identifier getByPosition(int index) {
		if (index < 0 || index >= this.infoTable.size())
			throw new IndexOutOfBoundsException("The provided index does not exist");
		
		Iterator<Long> it = this.byDateIndex.keySet().iterator();
		Long time = null;
		int i = -1;
		while (it.hasNext()) {
			time = it.next();
			i++;
			if (index == i) {
				break;
			}
		}
		int tableIndex = this.byDateIndex.get(time);
		return (Identifier) this.infoTable.get(tableIndex)[PID];
	}
	
	/**
	 * returns the size of the chain (count of the number of items)
	 * @return
	 */
	public int size() {
		return this.infoTable.size();
	}
	
	/**
	 * An ObsoletesChain is complete when the earliest and latest links have
	 * no obsoletedBy and obsoletes, respectively.
	 */
	public boolean isComplete() {
		if (this.infoTable.get(this.byIdIndex.get(getOriginalVersion()))[OBSOLETES] == null &&
			this.infoTable.get(this.byIdIndex.get(getLatestVersion()))[OBSOLETEDBY] == null) 
		{
			return true;
		}
		return false;
	}
	
	public Boolean isArchived(Identifier pid) {
		int tableIndex = this.byIdIndex.get(pid);
		Object[] oa = this.infoTable.get(tableIndex);
		return oa[IS_ARCHIVED] == null ? Boolean.FALSE : (Boolean) oa[IS_ARCHIVED];		
	}
	
	public Boolean latestIsArchived() {
		return isArchived(getLatestVersion());
	}
	
	public Date getPublishDate(Identifier pid) {
		int tableIndex = this.byIdIndex.get(pid);
		return (Date) this.infoTable.get(tableIndex)[PUBLISH_DATE];
	}
	
	public boolean isLatestVersion(Identifier pid) {
		if (this.infoTable.get(this.byIdIndex.get(pid))[OBSOLETEDBY] == null) {
			return true;
		}
		return false;
	}
}
