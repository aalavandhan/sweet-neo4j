require "RDF"
require "linkeddata"
require "neography"
require "active_support/all"

NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "admin"
SWEET_URL = "http://sweet.jpl.nasa.gov/2.3/sweetAll.owl"

class SweetParser

  def self.loadHandlers(handleConcepts, handleRelations, handleComments)
    @@cHandler  = handleConcepts
    @@rHandler = handleRelations
    @@comHandler  = handleComments
  end

  def initialize(path, imported)
    @path = path
    @imported = imported

    begin
      @graph = RDF::Graph.load(@path).to_a.drop(3)
    rescue
      puts "--- PARSE ERROR #{@path} --- "
      @graph = [ ]
    end
  end

  def imported?(element)
    @imported.include?(element.predicate.to_s)
  end

  def importable?(element)
    element.predicate.fragment.inquiry.imports? rescue false
  end

  def klass?(element)
    ( element.object.fragment.inquiry.Class? rescue false ) and
    ( element.subject.fragment rescue false )
  end

  def comment?(element)
    element.predicate.fragment.inquiry.comment? rescue false
  end

  def relationship?(element)
    not ( self.klass?(element) or self.importable?(element) ) and
    ( element.subject.fragment rescue false ) and
    ( element.predicate.fragment rescue false ) and
    ( element.object.fragment rescue false )
  end

  def import!(element)
    SweetParser.new(element.object.to_s, @imported + [ element.predicate.to_s ]).parse
  end

  def parser(element)
    self.import!(element)                      if self.importable?(element) and not self.imported?(element)

    @@cHandler.call(element.subject.fragment)   if self.klass?(element)

    @@rHandler.call(element.subject.fragment, element.predicate.fragment, element.object.fragment)  if self.relationship?(element)

    @@comHandler.call(element.subject.fragment, element.object.to_s)                                if self.comment?(element)
  end

  def parse
    @graph.each{ |e| self.parser(e) }
  end
end

Neography.configure do |config|
  config.http_send_timeout    = 3600
  config.http_receive_timeout = 3600
end


@neo = Neography::Rest.new({:authentication => 'basic', :username => NEO4J_USER, :password => NEO4J_PASSWORD })
@neo.create_unique_constraint("Concept", "name")

def create_node(concept)
  n  = @neo.create_node(:name => concept, :common_name => concept.underscore.humanize)
  @neo.add_label(n, "Concept") rescue @neo.delete_node(n)
  return n
end

conceptHandler = Proc.new do |c|
  create_node(c)
end

relationshipHandler = Proc.new do |c1, r, c2|
  n1 = @neo.find_nodes_labeled('Concept', :name => c1).first
  n2 = @neo.find_nodes_labeled('Concept', :name => c2).first
  n1 = create_node(c1) if not n1.present?
  n2 = create_node(c2) if not n2.present?

  @neo.create_relationship(r, n1, n2) rescue print " relationship creation error #{c1} -> #{r} -> #{c2}"
end

commentHandler = Proc.new do |c, comment|
  n = @neo.find_nodes_labeled('Concept', :name => c)
  n = create_node(c) if not n.present?
  @neo.set_node_properties(n, { :comment => comment })
end

SweetParser.loadHandlers(conceptHandler, relationshipHandler, commentHandler)

parser = SweetParser.new(SWEET_URL, [ ])
parser.parse
