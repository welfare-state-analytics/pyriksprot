<?xml version='1.0' encoding='UTF-8'?>
<!-- Extract speeches from ParlaClarin XML file -->
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tei="http://www.tei-c.org/ns/1.0"
  xmlns="http://www.tei-c.org/ns/1.0"
  xmlns:xi="http://www.w3.org/2001/XInclude"
  exclude-result-prefixes="tei xi">

  <xsl:strip-space elements="*"/>

  <xsl:output indent="yes" method="text" encoding="utf-8" />

  <xsl:template match="/">
      <protocol>
        <xsl:apply-templates/>
      </protocol>
  </xsl:template>
<!-- <xsl:if test="position() > 1"><xsl:text> </xsl:text></xsl:if>  -->
  <xsl:template match="tei:u">

    <xsl:variable name="next" select="@next"/>
    <xsl:variable name="prev" select="@prev"/>

    <xsl:choose>

        <xsl:when test="$prev='cont'">

            <xsl:apply-templates/>

            <xsl:if test="not(@prev) or $prev!='cont'">
              </speech>
            </xsl:if>

        </xsl:when>
        <xsl:otherwise>

            <speech who="{@who}" startid="{@xml:id}">

            <xsl:apply-templates/>

            <xsl:if test="not(@next) or $next!='cont'">
              <xsl:text ></speech></xsl:text>
            </xsl:if>

        </xsl:otherwise>

    </xsl:choose>


  </xsl:template>

  <xsl:template match="tei:seg">
    <xsl:value-of select="text()"></xsl:value-of>
    <xsl:text>&#10;</xsl:text>
  </xsl:template>

  <xsl:template match="tei:note">
  </xsl:template>

  <xsl:template match="tei:front">
  </xsl:template>

  <xsl:template match="tei:teiHeader">
  </xsl:template>

</xsl:stylesheet>
