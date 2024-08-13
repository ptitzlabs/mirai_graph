import os
import json
from collections import defaultdict

def merge_json_files(output_file="out/merged_output.json"):
    # Create defaultdicts to store the merged nodes and relationships
    merged_data = defaultdict(lambda: defaultdict(list))
    relationships = defaultdict(lambda: defaultdict(list))
    
    # Set to track existing node IDs to avoid duplicates
    node_ids = set()

    # First pass: Collect all nodes
    print("Starting first pass: Collecting nodes")
    for filename in os.listdir("data"):
        if filename.endswith(".json"):
            with open("data/"+ filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Merge nodes
                if "nodes" in data:
                    for node_type, nodes in data["nodes"].items():
                        for node in nodes:
                            node_id = node["id"]
                            if node_id not in node_ids:
                                merged_data["nodes"][node_type].append(node)
                                node_ids.add(node_id)
                            else:
                                for i, existing_node in enumerate(merged_data["nodes"][node_type]):
                                    if existing_node["id"] == node_id:
                                        # Merge properties
                                        if node_id == "ofii":
                                            pass
                                        existing_node.update(node)
                                        
                                        # Merge relationships if they exist in both nodes
                                        if "relationships" in node:
                                            if "relationships" not in existing_node:
                                                existing_node["relationships"] = node["relationships"]
                                                print(f"Added relationships to node {node_id}: {node['relationships']}")
                                            else:
                                                for rel_type, new_rels in node["relationships"].items():
                                                    if rel_type in existing_node["relationships"]:
                                                        existing_rels = {rel["id"]: rel for rel in existing_node["relationships"][rel_type]}
                                                        for new_rel in new_rels:
                                                            if new_rel["id"] in existing_rels:
                                                                existing_rels[new_rel["id"]].update(new_rel)
                                                                print(f"Updated existing relationship {new_rel['id']} for node {node_id}")
                                                            else:
                                                                existing_node["relationships"][rel_type].append(new_rel)
                                                                print(f"Added new relationship {new_rel['id']} to node {node_id}")
                                                    else:
                                                        existing_node["relationships"][rel_type] = new_rels
                                                        print(f"Added new relationship type {rel_type} for node {node_id}")
                                        
                                        merged_data["nodes"][node_type][i] = existing_node
                                        break

    # Second pass: Collect all relationships after nodes are aggregated
    print("Starting second pass: Collecting relationships")
    for filename in os.listdir("data"):
        if filename.endswith(".json"):
            with open("data/"+filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Collect relationships, checking if they refer to existing nodes
                if "relationships" in data:
                    for relationship_type, rels in data["relationships"].items():
                        for rel in rels:
                            source_id, target_id = rel.split(":")
                            # Sanity check: Only add relationship if both IDs exist
                            if source_id in node_ids and target_id in node_ids:
                                if rel not in relationships[relationship_type][source_id]:
                                    relationships[relationship_type][source_id].append(rel)
                                else:
                                  pass
                            else:
                                if source_id not in node_ids:
                                    print(f"Invalid {relationship_type} relationship dropped: {rel} in file {filename} (missing source node {source_id})")
                                else:
                                    print(f"Invalid {relationship_type} relationship dropped: {rel} in file {filename} (missing target node {target_id})")
                                    
    # Add relationships to the final merged data
    # Flatten relationships from nested dict structure
    for rel_type, rel_dict in relationships.items():
        if rel_type not in merged_data["relationships"]:
            merged_data["relationships"][rel_type] = []
        for rel_list in rel_dict.values():
            merged_data["relationships"][rel_type].extend(rel_list)

    # Write the merged data to the output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(merged_data, outfile, indent=2)

    print("Merging complete. Output written to:", output_file)

if __name__ == "__main__":
    merge_json_files()
