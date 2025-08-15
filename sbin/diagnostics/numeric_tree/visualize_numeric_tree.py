#!/usr/bin/python3
import networkx as nx
import json

# Try to import plotly for interactive visualization
try:
    import plotly.graph_objects as go
    import plotly.offline as pyo
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("Plotly not available. Install with: pip install plotly")
    print("Falling back to basic text output")


def group_consecutive_docs(documents):
    """
    Group consecutive document IDs with the same value into ranges.

    Args:
        documents: List of (value, doc_id) tuples

    Returns:
        List of dictionaries with keys:
        - 'value': the document value
        - 'start_id': first document ID in the group
        - 'end_id': last document ID in the group (same as start_id if single doc)
        - 'count': number of documents in the group
        - 'is_range': True if count > 1, False otherwise
    """
    if not documents:
        return []

    # Sort documents by value first, then by doc_id to group consecutive IDs with same value
    sorted_docs = sorted(documents, key=lambda x: (x[0], x[1]))

    groups = []
    current_group = None

    for value, doc_id in sorted_docs:
        if current_group is None:
            # Start first group
            current_group = {
                'value': value,
                'start_id': doc_id,
                'end_id': doc_id,
                'count': 1,
                'is_range': False
            }
        elif (current_group['value'] == value and
              doc_id == current_group['end_id'] + 1):
            # Extend current group with consecutive ID
            current_group['end_id'] = doc_id
            current_group['count'] += 1
            current_group['is_range'] = True
        else:
            # Start new group
            groups.append(current_group)
            current_group = {
                'value': value,
                'start_id': doc_id,
                'end_id': doc_id,
                'count': 1,
                'is_range': False
            }

    # Don't forget the last group
    if current_group is not None:
        groups.append(current_group)

    return groups


def draw_node(graph, node, total_doc_count):
    node_id = node['id']
    parent_id = node.get('parent_id')
    count = node.get('doc_count', 0)
    score = count / total_doc_count if total_doc_count > 0 else 0

    # Check if this is a leaf node with document values
    is_leaf = node.get('leaf', False)
    node_value = node.get('value', 0)

    # Store additional information for visualization
    node_info = {
        'value': node_value,
        'score': score,
        'is_leaf': is_leaf,
        'doc_count': count
    }

    # If it's a leaf node, store the document information
    if is_leaf and 'values' in node:
        node_info['documents'] = node['values']  # List of (value, doc_id) tuples
        node_info['num_docs'] = len(node['values'])

    graph.add_node(node_id, **node_info)

    if parent_id is not None:
        graph.add_edge(parent_id, node_id)

    # Only process children if this is NOT a leaf node
    # Leaf nodes should have one parent and no children
    if not is_leaf:
        if 'left' in node:
            draw_node(graph, node['left'], total_doc_count)
        if 'right' in node:
            draw_node(graph, node['right'], total_doc_count)

def draw_tree(root):
    G = nx.DiGraph()
    draw_node(G, root, root['doc_count'])
    return G

def calculate_subtree_width(G, node, parent=None, min_sibling_gap=2.0):
    """Calculate the minimum width needed for a subtree to avoid overlaps."""
    children = [child for child in G.neighbors(node) if child != parent]

    if not children:
        return 1.0  # Leaf node width

    if len(children) == 1:
        return calculate_subtree_width(G, children[0], node, min_sibling_gap)

    # For multiple children, sum their widths plus gaps
    child_widths = [calculate_subtree_width(G, child, node, min_sibling_gap) for child in children]
    total_child_width = sum(child_widths)
    total_gaps = (len(children) - 1) * min_sibling_gap

    return max(1.0, total_child_width + total_gaps)

def hierarchy_pos_improved(G, root=None, vert_gap=2.0, min_sibling_gap=4.0):
    """
    Create a hierarchical layout with proper sibling spacing that prevents overlaps.
    """
    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(iter(nx.topological_sort(G)))
        else:
            root = next(iter(G))

    # Calculate the total width needed for the entire tree
    total_width = calculate_subtree_width(G, root, None, min_sibling_gap)

    def _place_nodes(G, node, parent=None, x_center=0, y_pos=0, allocated_width=None):
        pos = {}

        if allocated_width is None:
            allocated_width = total_width

        # Place current node
        pos[node] = (x_center, y_pos)

        # Get children
        children = [child for child in G.neighbors(node) if child != parent]

        if not children:
            return pos

        if len(children) == 1:
            # Single child: place directly below
            child_pos = _place_nodes(G, children[0], node, x_center, y_pos - vert_gap, allocated_width)
            pos.update(child_pos)
        else:
            # Multiple children: calculate positions with proper spacing
            child_widths = [calculate_subtree_width(G, child, node, min_sibling_gap) for child in children]

            # Calculate starting position for leftmost child
            total_child_width = sum(child_widths)
            total_gaps = (len(children) - 1) * min_sibling_gap
            total_needed = total_child_width + total_gaps

            start_x = x_center - total_needed / 2
            current_x = start_x

            for i, child in enumerate(children):
                child_width = child_widths[i]
                child_center = current_x + child_width / 2

                child_pos = _place_nodes(G, child, node, child_center, y_pos - vert_gap, child_width)
                pos.update(child_pos)

                current_x += child_width + min_sibling_gap

        return pos

    return _place_nodes(G, root)

def create_interactive_plotly_tree(G, output_file='interactive_tree.html', spacing_factor=2.0):
    """Create an interactive tree visualization using Plotly."""
    if not PLOTLY_AVAILABLE:
        print("Plotly not available. Please install with: pip install plotly")
        return False

    print("Creating interactive visualization...")

    # Try different layout algorithms for better sibling spacing
    try:
        # First try graphviz if available
        pos = nx.nx_agraph.graphviz_layout(G, prog='dot', args=f'-Granksep={2.0 * spacing_factor} -Gnodesep={4.0 * spacing_factor}')
        print("Using Graphviz layout")
    except:
        try:
            # Fallback to our improved layout
            min_sibling_gap = 6.0 * spacing_factor
            vert_gap = 2.0 * spacing_factor
            pos = hierarchy_pos_improved(G, vert_gap=vert_gap, min_sibling_gap=min_sibling_gap)
            print("Using custom hierarchical layout")
        except:
            # Final fallback to spring layout with good spacing
            pos = nx.spring_layout(G, k=3*spacing_factor, iterations=50, scale=10*spacing_factor)
            print("Using spring layout")

    # Extract node and edge information
    node_x = []
    node_y = []
    node_text = []
    node_colors = []

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)

        node_info = G.nodes[node]
        value = node_info.get('value', 'N/A')
        score = node_info.get('score', 0)
        is_leaf = node_info.get('is_leaf', False)
        doc_count = node_info.get('doc_count', 0)

        # Create hover text with detailed information
        hover_text = f"Node ID: {node}<br>Value: {value}<br>Score: {score:.3f}<br>Doc Count: {doc_count}"

        # If it's a leaf node, show document information
        if is_leaf and 'documents' in node_info:
            documents = node_info['documents']
            hover_text += f"<br><br><b>LEAF NODE - Documents:</b>"

            # Group consecutive document IDs with the same value into ranges
            grouped_docs = group_consecutive_docs(documents)

            # Show up to 10 groups/ranges to avoid overwhelming the tooltip
            displayed_count = 0
            total_docs_shown = 0

            for group in grouped_docs:
                if displayed_count >= 10:
                    break

                if group['is_range']:
                    hover_text += f"<br>Docs {group['start_id']}-{group['end_id']} ({group['count']} docs): {group['value']}"
                else:
                    hover_text += f"<br>Doc {group['start_id']}: {group['value']}"

                displayed_count += 1
                total_docs_shown += group['count']

            if total_docs_shown < len(documents):
                hover_text += f"<br>... and {len(documents) - total_docs_shown} more documents"

        node_text.append(hover_text)
        node_colors.append(score)

    # Create edges
    edge_x = []
    edge_y = []

    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # Create the plot
    fig = go.Figure()

    # Add edges
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=2, color='#888'),
        hoverinfo='none',
        mode='lines',
        name='Edges'
    ))

    # Add nodes
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        hovertext=node_text,
        text=[f"🍃{node}" if G.nodes[node].get('is_leaf', False) else f"ID:{node}" for node in G.nodes()],
        textposition="middle center",
        textfont=dict(size=8, color="white"),
        marker=dict(
            size=20,
            color=node_colors,
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(
                title="Score",
                tickmode="linear",
                tick0=0,
                dtick=0.1
            ),
            line=dict(width=2, color='white')
        ),
        name='Nodes'
    ))

    # Update layout for better interactivity
    fig.update_layout(
        title=dict(
            text="Interactive Numeric Tree Visualization",
            x=0.5,
            font=dict(size=20)
        ),
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        annotations=[ dict(
            text="Zoom, pan, and hover over nodes for details",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.005, y=-0.002,
            xanchor='left', yanchor='bottom',
            font=dict(color="#888", size=12)
        )],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white'
    )

    # Save as HTML file
    pyo.plot(fig, filename=output_file, auto_open=False)
    print(f"Interactive visualization saved to {output_file}")
    print("Open the HTML file in your browser to interact with the tree")
    return True



def print_tree_info(G):
    """Print basic information about the tree."""
    print(f"Tree has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")

    # Find root (node with no predecessors)
    root = None
    for node in G.nodes():
        if G.in_degree(node) == 0:
            root = node
            break

    if root is not None:
        print(f"Root node: {root}")
        node_info = G.nodes[root]
        print(f"Root value: {node_info.get('value', 'N/A')}")
        print(f"Root score: {node_info.get('score', 'N/A')}")

    # Print some sample nodes
    print("\nSample nodes:")
    leaf_count = 0
    internal_count = 0

    for node in list(G.nodes())[:10]:
        node_info = G.nodes[node]
        score = node_info.get('score', 0)
        is_leaf = node_info.get('is_leaf', False)
        doc_count = node_info.get('doc_count', 0)

        if is_leaf:
            leaf_count += 1
            documents = node_info.get('documents', [])
            print(f"  🍃 LEAF Node {node}: value={node_info.get('value', 'N/A')}, docs={len(documents)}")
            if documents:
                # Group consecutive documents and show first few groups
                grouped_docs = group_consecutive_docs(documents)
                total_docs_shown = 0

                for group in grouped_docs[:3]:  # Show first 3 groups
                    if group['is_range']:
                        print(f"    - Docs {group['start_id']}-{group['end_id']} ({group['count']} docs): {group['value']}")
                    else:
                        print(f"    - Doc {group['start_id']}: {group['value']}")
                    total_docs_shown += group['count']

                if len(grouped_docs) > 3:
                    remaining_docs = len(documents) - total_docs_shown
                    print(f"    - ... and {remaining_docs} more documents in {len(grouped_docs) - 3} more groups")
        else:
            internal_count += 1
            if isinstance(score, (int, float)):
                print(f"  🌳 Node {node}: value={node_info.get('value', 'N/A')}, score={score:.6f}, doc_count={doc_count}")
            else:
                print(f"  🌳 Node {node}: value={node_info.get('value', 'N/A')}, score={score}, doc_count={doc_count}")

    if G.number_of_nodes() > 10:
        print(f"  ... and {G.number_of_nodes() - 10} more nodes")

    print(f"\nTree summary: {leaf_count} leaf nodes, {internal_count} internal nodes shown")

if __name__ == "__main__":
    import sys

    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Interactive visualization: python numeric_tree_visualize.py <tree.json> [spacing_factor]")
        print("  Tree info only:           python numeric_tree_visualize.py <tree.json> info")
        print("")
        print("Examples:")
        print("  python numeric_tree_visualize.py tree.json 3.0")
        print("  python numeric_tree_visualize.py tree.json info")
        sys.exit(1)

    input_file = sys.argv[1]

    # Load the tree
    try:
        tree = json.load(open(input_file, 'r'))
        G = draw_tree(tree)
        print(f"Successfully loaded tree from {input_file}")
    except Exception as e:
        print(f"Error loading tree: {e}")
        sys.exit(1)

    # Check if user wants info only
    if len(sys.argv) > 2 and sys.argv[2].lower() == 'info':
        print_tree_info(G)
        sys.exit(0)

    # Interactive visualization
    spacing_factor = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    output_file = 'interactive_tree.html'

    print(f"Creating interactive visualization with spacing factor: {spacing_factor}")

    if PLOTLY_AVAILABLE:
        try:
            success = create_interactive_plotly_tree(G, output_file, spacing_factor)
            if success:
                print(f"Interactive visualization saved to {output_file}")
                print("Open the HTML file in your browser to explore the tree")
        except Exception as e:
            import traceback
            print(f"Error creating interactive visualization: {e}")
            traceback.print_exc()
            print("\nFalling back to tree info:")
            print_tree_info(G)
    else:
        print("Plotly not available. Showing tree information instead:")
        print_tree_info(G)