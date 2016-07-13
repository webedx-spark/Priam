package com.netflix.priam.identity;

public class DcAndRac
{
    private final String dc;
    private final String rac;

    public DcAndRac(String dc, String rac)
    {
        this.dc = dc;
        this.rac = rac;
    }

    public String getDc()
    {
        return dc;
    }

    public String getRac()
    {
        return rac;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DcAndRac dcAndRac = (DcAndRac) o;

        if (!dc.equals(dcAndRac.dc)) return false;
        if (!rac.equals(dcAndRac.rac)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = dc.hashCode();
        result = 31 * result + rac.hashCode();
        return result;
    }
}
