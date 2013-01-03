package edu.ohsu.sonmezsysbio.cloudbreak;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/9/12
 * Time: 9:59 AM
 */
public class ReadGroupInfo {
    public String readGroupName;
    public String libraryName;
    public int isize;
    public int isizeSD;
    public boolean matePair;
    public String hdfsPath;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReadGroupInfo that = (ReadGroupInfo) o;

        if (isize != that.isize) return false;
        if (isizeSD != that.isizeSD) return false;
        if (matePair != that.matePair) return false;
        if (hdfsPath != null ? !hdfsPath.equals(that.hdfsPath) : that.hdfsPath != null) return false;
        if (libraryName != null ? !libraryName.equals(that.libraryName) : that.libraryName != null) return false;
        if (readGroupName != null ? !readGroupName.equals(that.readGroupName) : that.readGroupName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = readGroupName != null ? readGroupName.hashCode() : 0;
        result = 31 * result + (libraryName != null ? libraryName.hashCode() : 0);
        result = 31 * result + isize;
        result = 31 * result + isizeSD;
        result = 31 * result + (matePair ? 1 : 0);
        result = 31 * result + (hdfsPath != null ? hdfsPath.hashCode() : 0);
        return result;
    }
}
