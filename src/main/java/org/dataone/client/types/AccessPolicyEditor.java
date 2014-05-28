package org.dataone.client.types;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v1.util.AuthUtils;
import org.dataone.service.util.Constants;

public class AccessPolicyEditor implements Cloneable {
	
	private AccessPolicy policy;
	
	public AccessPolicyEditor(AccessPolicy policy)
	{
		if (policy == null) 
			this.policy = new AccessPolicy();
		else
			this.policy = policy;
	}

	/**
	 * gets the AccessPolicy being manipulated
	 * @return
	 */
	public AccessPolicy getAccessPolicy()
	{
		return this.policy;
	}
	
	/**
	 * For the given Subjects, adds the permission specified unless the matching
	 * or a 'greater' permission is already there. 
	 * (where CHANGE_PERMISSION is greater than WRITE is greater than READ) 
	 * @param subjects
	 * @param permission
	 */
	public void addAccess(Subject[] subjects, Permission permission)
	{
		AccessRule newRule = null;
		for (Subject s: subjects) {
			Set<Permission> perms = AccessUtil.getPermissionMap(this.policy).get(s);
			if (perms == null) {
				if (newRule == null) {
					newRule = new AccessRule();
					newRule.addPermission(permission);
				}
				newRule.addSubject(s);
			} else {
				if (AuthUtils.comparePermissions(permission, perms)) {
					; // already allowed
				}
				else {
					removeAccess(new Subject[]{s});
					newRule.addSubject(s);
				}
			}
		}
		policy.addAllow(newRule);
	}
	
	
	/**
	 * Removes the specified Subjects from the AccessPolicy
	 * @param subjects
	 * @return
	 */
	public boolean removeAccess(Subject[] subjects) 
	{
		List<Subject> subjectList = Arrays.asList(subjects);
		boolean accessChanged = false;
		for (int i = 0; i < this.policy.sizeAllowList(); i++) {
			AccessRule ar = this.policy.getAllow(i);
			if (ar.getSubjectList().removeAll(subjectList)) {
				accessChanged = true;
				if (ar.getSubjectList().isEmpty()) {
					this.policy.getAllowList().remove(i);
				}
			}
		}
		return accessChanged;
	}
	
	
	public void setAccess(Subject[] subjects, Permission permission)
	{
		// TODO: prefer not to disrupt existing access rules if possible
		// so consider refactoring

		
		removeAccess(subjects);
		AccessRule newRule = AccessUtil.createAccessRule(subjects,
				new Permission[]{permission});
		this.policy.addAllow(newRule);
	}
	
	
	public void clearAccessPolicy()
	{
		this.policy.clearAllowList();
	}
	
	public AccessPolicy clone()
	{
		return AccessUtil.cloneAccessPolicy(this.policy);
	}
	
	
	public boolean hasAccess(Subject subject, Permission permission)
	{
		return AuthUtils.comparePermissions(permission,
				AccessUtil.getPermissionMap(this.policy).get(subject));
	}
	
		
	public void setPublicAccess()
	{
		Subject publick = new Subject();
    	publick.setValue(Constants.SUBJECT_PUBLIC);
		
    
    	if (! hasAccess(publick, Permission.READ)) {
    		// add a new rule giving public access
    		AccessRule ar = new AccessRule();
    		ar.addSubject(publick);
    		ar.addPermission(Permission.READ);
    		this.policy.addAllow(ar);
    	}
	}
}
