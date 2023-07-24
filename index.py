from flask import Flask,jsonify,request
from neo4j import GraphDatabase

app = Flask(__name__)

# Configuration for connecting to the Neo4j database
neo4j_uri = "bolt://localhost:7687"  # Replace with your Neo4j database URI
neo4j_user = "neo4j"                 # Replace with your Neo4j username
neo4j_password = "Domainexpansion"   # Replace with your Neo4j password

# Function to create a Neo4j driver instance
def get_neo4j_driver():
    return GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

@app.route('/')
def index():
    with get_neo4j_driver().session() as session:
        # Perform Neo4j queries and process data here
        result = session.run("MATCH (n) RETURN n LIMIT 10")
        nodes = [record['n'] for record in result]

        # Convert the Node objects to dictionaries
        nodes_data = [dict(node) for node in nodes]

    # Return the list of dictionaries as JSON
    return jsonify(nodes_data)


#1.Insert the new film and show 
@app.route('/film', methods=['POST'])
def insert_film():
    data = request.get_json()
    film_id = data.get('film_id')
    title = data.get('title')
    description = data.get('description')
    rental_duration = data.get('rental_duration')
    rental_rate = data.get('rental_rate')
    length = data.get('length')
    replacement_cost = data.get('replacement_cost')
    rating = data.get('rating')

    with get_neo4j_driver().session() as session:
        # Create a new film node
        query = (
            "CREATE (f:Film {film_id: $film_id, title: $title, description: $description, "
            "rental_duration: $rental_duration, rental_rate: $rental_rate, length: $length, "
            "replacement_cost: $replacement_cost, rating: $rating}) RETURN f"
        )
        result = session.run(query, film_id=film_id, title=title, description=description,
                             rental_duration=rental_duration, rental_rate=rental_rate,
                             length=length, replacement_cost=replacement_cost, rating=rating)
        film_node = result.single()['f']

    return jsonify({"message": "Film inserted successfully", "film": dict(film_node)})

#To show the Film for the paticular film id 
@app.route('/film/<int:film_id>', methods=['GET'])
def get_film(film_id):
    with get_neo4j_driver().session() as session:
        # Retrieve the film node
        query = "MATCH (f:Film {film_id: $film_id}) RETURN f"
        result = session.run(query, film_id=film_id)
        film_node = result.single()

    if film_node:
        return jsonify(dict(film_node['f']))
    else:
        return jsonify({"message": "Film not found"}), 404

    

#2.	Update the film and show information using title. (By update only title, description, and rating)

@app.route('/film/<string:fname>', methods=['PATCH'])
def update_film(fname):
    data = request.get_json()
    title = data.get('title')
    description = data.get('description')
    rating = data.get('rating')

    with get_neo4j_driver().session() as session:
        # Update the film node
        query = (
            "MATCH (f:Film {title: $fname}) "
            "SET f.title = $title, f.description = $description, f.rating = $rating "
            "RETURN f"
        )
        result = session.run(query, fname=fname, title=title, description=description, rating=rating)
        film_node = result.single()

    if film_node:
        # Extract the updated film details from the result
        updated_film = {
            "title": film_node["f"]["title"],
            "description": film_node["f"]["description"],
            "rating": film_node["f"]["rating"]
        }
        return jsonify(updated_film)
    else:
        return jsonify({"message": "Film not found"}), 404
    
#3.	Delete the film and show information using title.

@app.route('/film/<string:fname>', methods=['DELETE'])
def delete_film(fname):
    with get_neo4j_driver().session() as session:
        # Delete the film node
        query = (
            "MATCH (f:Film {title: $fname}) "
            "DELETE f"
        )
        session.run(query, fname=fname)

    return jsonify({"message": "Film deleted successfully"})
    
#4.	Retrieve all the films and shows in database.
@app.route('/film', methods=['GET'])
def get_all_films():
    with get_neo4j_driver().session() as session:
        # Retrieve all film nodes
        query = "MATCH (f:Film) RETURN f"
        result = session.run(query)
        films = [dict(record['f']) for record in result]

    return jsonify(films)


#5.	Display the film and show’s detail includes actor’ names.
@app.route('/film/<string:fname>', methods=['GET'])
def get_film_details(fname):
    # Trim the input title and convert to lowercase
    fname = fname.strip().lower()

    with get_neo4j_driver().session() as session:
        # Retrieve film and show details including actor names
        query = (
            "MATCH (f:Film) "
            "WHERE toLower(f.title) = $fname "
            "OPTIONAL MATCH (f)<-[:ACTED_IN]-(a:Actor) "
            "RETURN f.title AS title, f.description AS description, "
            "f.rental_duration AS rental_duration, f.rental_rate AS rental_rate, "
            "f.length AS length, f.replacement_cost AS replacement_cost, "
            "f.rating AS rating, COLLECT(DISTINCT a.name) AS actors"
        )
        result = session.run(query, fname=fname)

        film_details = result.single()

    if film_details:
        # Convert actor names to lowercase for consistency
        actors = [actor_name.lower() for actor_name in film_details["actors"]]

        # Create a dictionary to store the film and show details with actor names
        film_info = {
            "title": film_details["title"],
            "description": film_details["description"],
            "rental_duration": film_details["rental_duration"],
            "rental_rate": film_details["rental_rate"],
            "length": film_details["length"],
            "replacement_cost": film_details["replacement_cost"],
            "rating": film_details["rating"],
            "actors": actors
        }

        return jsonify(film_info)
    else:
        return jsonify({"message": "Film not found"}), 404
       

if __name__ == '__main__':
    port = 8000
    # Run the Flask app with the specified port
    app.run(port=port,debug=True)