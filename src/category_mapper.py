class CategoryMapper:
    def __init__(self):
        # Define category keywords based on Gumroad's categories
        self.category_mapping = {
            'Design': ['template', 'UI kit', 'mockup', 'design asset', 'Figma', 'web design', 'graphic design', 'logo', 'branding', 'typography'],
            
            'Drawing & Painting': ['drawing', 'painting', 'illustration', 'sketch', 'art', 'procreate', 'brush', 'digital painting', 'concept art', 'character design'],
            
            '3D': ['3D model', 'blender', 'maya', '3D asset', 'texture', 'rendering', 'animation', 'character model', 'sculpting', 'zbrush'],
            
            'Self Improvement': ['personal development', 'productivity', 'mindfulness', 'meditation', 'self help', 'motivation', 'habit', 'lifestyle', 'wellness'],
            
            'Music & Sound Design': ['music', 'sound effect', 'audio', 'beat', 'sample pack', 'sound kit', 'drum kit', 'soundtrack', 'mixing', 'mastering'],
            
            'Films': ['video', 'film', 'cinema', 'documentary', 'movie', 'screenplay', 'filmmaking', 'editing', 'production'],
            
            'Software': ['plugin', 'script', 'app', 'software', 'tool', 'extension', 'code', 'program', 'application', 'development'],
            
            'Business & Money': ['business plan', 'marketing', 'finance', 'investment', 'entrepreneurship', 'strategy', 'sales', 'ecommerce', 'startup'],
            
            'Education': ['course', 'tutorial', 'guide', 'training', 'workshop', 'lesson', 'masterclass', 'learning', 'teaching'],
            
            'Gaming': ['game', 'asset pack', 'unity', 'unreal', 'gaming', 'mod', 'game design', 'level design', 'rpg'],
            
            'Photography': ['photo', 'preset', 'lightroom', 'photography', 'camera', 'filter', 'photoshop', 'editing', 'portrait'],
            
            'Fitness & Health': ['workout', 'fitness', 'health', 'nutrition', 'exercise', 'diet', 'yoga', 'training plan', 'wellness'],
            
            'Comic & Graphic Novels': ['comic', 'manga', 'graphic novel', 'storyboard', 'sequential art', 'superhero', 'webtoon'],
            
            'Fiction Books': ['novel', 'fiction', 'story', 'book', 'fantasy', 'sci-fi', 'romance', 'thriller', 'adventure'],
            
            'Audio': ['podcast', 'audiobook', 'voice over', 'audio course', 'music production', 'sound recording'],
            
            'Writing & Publishing': ['writing guide', 'publishing', 'author', 'manuscript', 'book writing', 'editing', 'storytelling']
        }

    def detect_category(self, title: str, description: str) -> str:
        """Detect category based on keywords in title and description"""
        # Combine title and description, convert to lowercase for matching
        text = f"{title} {description}".lower()
        
        # Store matches with their counts and confidence scores
        category_matches = {}
        
        # Check for keywords in each category
        for category, keywords in self.category_mapping.items():
            matches = 0
            confidence = 0
            
            for keyword in keywords:
                keyword_lower = keyword.lower()
                # Check exact matches
                if keyword_lower in text:
                    matches += 1
                    # Add extra weight for keywords found in title
                    if keyword_lower in title.lower():
                        confidence += 2
                    else:
                        confidence += 1
                        
            if matches > 0:
                category_matches[category] = confidence
        
        # If we found matches, return the category with highest confidence
        if category_matches:
            return max(category_matches.items(), key=lambda x: x[1])[0]
        
        # If no matches found, return "Other"
        return "Other"