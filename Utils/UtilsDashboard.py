import dash # type: ignore
from dash import dcc, html # type: ignore
from dash.dependencies import Input, Output # type: ignore
import plotly.graph_objs as go # type: ignore

class UtilsDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__)
        self.map_data = None
        self.bar_chart_data = None
        self.country_mapping = None  

        """
        T1 - V1 - V1H1 | V1H2 | V1H3 | V1H4|
             V2 - V2H1 | V2H2 - V2H2V1 | V2H3
                                V2H2V2
        
        """

        # Initialize the layout of the Dash app
        self.app.layout = html.Div([
            # Top Container
            html.Div([

                html.Div([
                    html.H1(id='head-name', style={'textAlign': 'center', 'color': '#0a0a0a', 'fontSize': '16px'}),
                    html.H1(id='noc-name', style={'textAlign': 'center', 'color': '#ffffff', 'fontSize': '14px'})
                    ], 
                    # style of top vertical container V1H1
                    style={'display': 'inline','backgroundColor': '#78a2cc','margin-left':'10px', 'justify-content': 'left', 'align-items': 'center', 'width': '50%','height': '80%','borderRadius': '10px', 'border': '1px solid black'})
                ,
                

                html.Div([
                    html.H1(id='silver-head-name', style={'textAlign': 'center', 'color': '#0a0a0a', 'fontSize': '16px'}),
                    html.H1(id='silver-noc-name', style={'textAlign': 'center', 'color': '#ffffff', 'fontSize': '14px'})
                    ],
                    # style of top vertical container V1H2 
                    style={'display': 'inline','backgroundColor': '#78a2cc','margin-left':'10px', 'justify-content': 'left', 'align-items': 'center', 'width': '50%','height': '80%','borderRadius': '10px', 'border': '1px solid black'}),


                html.Div([
                    html.H1(id='bronze-head-name', style={'textAlign': 'center', 'color': '#0a0a0a', 'fontSize': '16px'}),
                    html.H1(id='bronze-noc-name', style={'textAlign': 'center', 'color': '#ffffff', 'fontSize': '14px'})
                    ], 
                    # style of top vertical container V1H3
                    style={'display': 'inline','backgroundColor': '#78a2cc','margin-left':'10px', 'justify-content': 'left', 'align-items': 'center', 'width': '50%','height': '80%','borderRadius': '10px', 'border': '1px solid black'}),


                html.Div([
                    html.H1(id='total-head-name', style={'textAlign': 'center', 'color': '#0a0a0a', 'fontSize': '16px'}),
                    html.H1(id='total-noc-name', style={'textAlign': 'center', 'color': '#ffffff', 'fontSize': '14px'})
                    ], 
                    # style of top vertical container V1H4
                    style={'display': 'inline','backgroundColor': '#78a2cc','margin-left':'10px', 'justify-content': 'left', 'align-items': 'center', 'width': '50%','height': '80%','borderRadius': '10px', 'border': '1px solid black'})
                    
                # style of top vertical container V1
                ], style={'display': 'flex', 'height': '15%', 'justify-content': 'left', 'align-items': 'center','backgroundColor': '#2C3E50'}),  # Main container


            # container V2H1
            html.Div([
                html.Div([
                    dcc.Graph(id='bar-chart'),  # Bar chart div
                    ], 
                    # style of top vertical container V2H1
                    style={'width': '40%','height': '100%',  'justify-content': 'center', 'align-items': 'top', 'border': '1px solid black'}),  # Adjust width and add padding



                html.Div([
                    html.Div([
                        dcc.Graph(id='world-map'),  # Map chart div
                        ], 
                        # style of top vertical container V2H2V1
                        style={'height': '70%', 'justify-content': 'center', 'align-items': 'top'}),  # Adjust width and add padding
                    html.Div([
                        html.Table(id='top-10-table')  # Table div for top 10 data,  # Map chart div
                        ], 
                        # style of top vertical container V2H2V2
                        style={'width': '100%','height': '30%','margin-top':'30px','margin-left':'140px','margin-right':'30px','justify-content': 'center'}),  # Adjust width and add padding
                    ], 
                    # style of top vertical container V2H2
                    style={'display': 'inline','width': '50%',  'justify-content': 'center',  'border': '1px solid black'}),


                html.Div([
                    dcc.Dropdown(
                        id='column-dropdown',
                        options=[
                            {'label': 'Gold', 'value': 'gold_count'},
                            {'label': 'Silver', 'value': 'silver_count'},
                            {'label': 'Bronze', 'value': 'bronze_count'},
                            {'label': 'Total Medal', 'value': 'total_count'}
                        ],
                        value='gold_count', # Make dropdown full width
                        style={'width': '100%'}
                    )
                    ], 
                    # style of top vertical container V2H3
                    style={'backgroundColor': '#2C3E50','width': '10%','height': '100%',  'justify-content': 'center', 'align-items': 'top', 'border': '1px solid black'})

                ], 
                # style of top vertical container V2
                style={'display': 'flex','height': '92%', 'justify-content': 'space-between'})
            
            # style of top vertical container t1
            ], style={'height': '90vh','padding': '5px'})
        
        # Define the callbacks with the class method
        @self.app.callback(
            [
                Output('head-name', 'children'),
                Output('noc-name', 'children'),
                Output('silver-head-name', 'children'),
                Output('silver-noc-name', 'children'),
                Output('bronze-head-name', 'children'),
                Output('bronze-noc-name', 'children'),
                Output('total-head-name', 'children'),
                Output('total-noc-name', 'children')
                ],
            [Input('column-dropdown', 'value')]
        )
        def update_noc(selected_column):
            if self.most_gold_data is None:
                return ''

            # Get the data for the selected 'noc'
            head_name  = f"Most Number of Gold Medal"
            gn= self.most_gold_data.loc[0, 'noc']
            gc =self.most_gold_data.loc[0, 'gold_count']
            noc_name = f"{gn} : {gc} medals"

            silver_head_name  = f"Most Number of Silver Medal"
            sn = self.most_silver_data.loc[0, 'noc']
            sc = self.most_silver_data.loc[0, 'silver_count']
            silver_noc_name = f"{sn} : {sc} medals"

            bronze_head_name  = f"Most Number of Bronze Medal"
            bn= self.most_bronze_data.loc[0, 'noc']
            bc = self.most_bronze_data.loc[0, 'bronze_count']
            bronze_noc_name = f"{bn} : {bc} medals"

            # Get the data for the selected 'noc'
            total_head_name  = f"Most Number of Medals"
            tn = self.most_total_medal_data.loc[0, 'noc']
            tc = self.most_total_medal_data.loc[0, 'total_count']
            total_noc_name = f"{tn} : {tc} medals"
            
            return head_name,noc_name,silver_head_name,silver_noc_name,bronze_head_name,bronze_noc_name,total_head_name,total_noc_name
        


        @self.app.callback(
            Output('world-map', 'figure'),
            [Input('column-dropdown', 'value')]
        )
        def update_map(selected_column):
            if self.map_data is None or self.country_mapping is None:
                return go.Figure()

            selected_data = self.map_data.replace({'noc': self.country_mapping})[['noc', selected_column]]

            world_map = go.Figure(data=go.Choropleth(
                locations=selected_data['noc'],
                z=selected_data[selected_column],
                text=selected_data['noc'],
                colorscale='Viridis',
                autocolorscale=False,
                reversescale=True,
                marker_line_color='darkgray',
                marker_line_width=0.1
            ))

            world_map.update_layout(
                title=dict(
                    text=f'Medal Count by Country - {selected_column}',
                    x=0.5,  # Padding
                ),
                geo=dict(
                    showcoastlines=True,
                    projection_type='equirectangular'
                )
            )

            return world_map

        @self.app.callback(
            Output('bar-chart', 'figure'),
            [Input('column-dropdown', 'value')]
        )
        def update_bar_chart(selected_column):
            if self.bar_chart_data is None:
                return go.Figure()

            bar_chart = go.Figure(data=[
                go.Bar(
                    x=-self.bar_chart_data['female_count'],
                    y=self.bar_chart_data['discipline'],
                    name='Female count',
                    orientation='h',
                    marker=dict(color='rgba(255, 0, 0, 0.6)')
                ),
                go.Bar(
                    x=self.bar_chart_data['male_count'],
                    y=self.bar_chart_data['discipline'],
                    name='Male count',
                    orientation='h',
                    marker=dict(color='rgba(0, 0, 255, 0.6)')
                )
            ])
            num_disciplines = len(self.bar_chart_data['discipline'])
            chart_height = max(400, num_disciplines * 13)

            bar_chart.update_layout(
                title=dict(
                    text="Discipline Gender Ratio",
                    x=0.5,  # Padding
                    font = dict(color="#2b3e50",size = 20,family = 'Arial'),
                ),
                xaxis_title='Count',
                yaxis_title='Discipline',
                yaxis=dict(autorange="reversed"),
                barmode='relative',
                bargap=0.1,
                bargroupgap=0.1,
                xaxis=dict(
                    tickmode='array',
                    title_text='Count',
                    zeroline=True,
                    zerolinewidth=2,
                    zerolinecolor='black'
                ),
                legend=dict(x=0.7, y=1),
                #margin=dict(l=30, r=0, t=30, b=0),
                plot_bgcolor='rgba(0, 0, 0, 0)',
                height=chart_height
            )

            return bar_chart

        # Callback to update the top 10 table
        @self.app.callback(
            Output('top-10-table', 'children'),
            [Input('column-dropdown', 'value')]
        )
        def update_top_5_table(selected_column):
            if self.map_data is None or self.country_mapping is None:
                return []

            selected_data = self.map_data.replace({'noc': self.country_mapping})[['noc', selected_column]]
            top_5_data = selected_data.nlargest(5, selected_column)

            # Define styles for the table and its elements
            table_style = {
                'width': '30vw',    # Set table width to 100%  
                'borderCollapse': 'collapse',   # Collapse borders to remove gaps between cells
               # Center-align the table
            }

            header_style = {
                'backgroundColor': '#f2f2f2',   # Light gray background color
                'fontWeight': 'bold',           # Bold text
                'textAlign': 'center',          # Center-aligned text
                'border': '1px solid black'     # Add border line for header cells
            }

            cell_style = {
                'border': '1px solid black',    # Add border line for cells               # Add padding to cells
                'textAlign': 'center'           # Center-align cell content
            }

            table = html.Div(
                html.Table(
                    # Header with styled cells
                    [html.Tr([html.Th(col, style=header_style) for col in top_5_data.columns])] +                # Body
                    [html.Tr([
                        html.Td(top_5_data.iloc[i][col], style=cell_style) for col in top_5_data.columns
                    ], style={'border': '1px solid black'}) for i in range(min(len(top_5_data), 5))],
                    style=table_style
                ),
                style={'text-align': 'center'}  # Center-align the table
            )

            return table




    def set_input_data(self,map_data_pandas,bar_chart_data_pandas,most_gold_data_pandas,most_silver_data_pandas,most_bronze_data_pandas,most_total_medal_data_pandas,plotly_map):
        self.map_data = map_data_pandas
        self.bar_chart_data = bar_chart_data_pandas
        self.most_gold_data = most_gold_data_pandas
        self.most_silver_data = most_silver_data_pandas
        self.most_bronze_data = most_bronze_data_pandas
        self.most_total_medal_data = most_total_medal_data_pandas
        self.country_mapping = plotly_map  

    def run_server(self):
        self.app.run_server(debug=True)



