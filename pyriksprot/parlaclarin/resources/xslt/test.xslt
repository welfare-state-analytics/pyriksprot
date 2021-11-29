<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tei="http://www.tei-c.org/ns/1.0"
  xmlns="http://www.tei-c.org/ns/1.0"
  xmlns:xi="http://www.w3.org/2001/XInclude"
  exclude-result-prefixes="tei xi">
<xsl:output method='xml' version='1.0' encoding='utf-8' indent='yes'/>

<xsl:template match="/tei:teiCorpus/tei:TEI/tei:text/tei:body/tei:div">
    <output>
        <!-- <xsl:apply-templates
        select="tei:u[count(preceding-sibling::tei:u[not(@prev='cont')])
        count(preceding-sibling::tei:u[not(@prev='cont')])]"/> -->
        <xsl:apply-templates/>
    </output>
</xsl:template>


<!--
<xsl:template match="a[not(@type='start')]">
    <single><xsl:value-of select="t" /></single>
</xsl:template>
-->

<xsl:template match="tei:u">
   <xsl:number from="tei:u[@next='count' or not(@next)]" level="any" />
</xsl:template>

<!--
<xsl:template match="a" mode="merge">
    <xsl:value-of select="t" />
</xsl:template> -->

  <xsl:template match="tei:teiHeader">
  </xsl:template>

  <xsl:template match="tei:seg">
  </xsl:template>

  <xsl:template match="tei:front">
  </xsl:template>

  <xsl:template match="tei:note">
  </xsl:template>
</xsl:stylesheet>